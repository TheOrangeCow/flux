import socket
import threading
import time
import traceback
import uuid

from .events import Events, arity
from .protocol import (ConnectionClosed, Reader, TCPTransport, WSTransport,
                       decode, encode, ws_server_handshake)

_MISSING = object()


class ServerClient:
    """One connected peer (TCP or WebSocket) as seen by the server."""

    def __init__(self, server, transport, address):
        self.server = server
        self.transport = transport
        self.address = address
        self.id = uuid.uuid4().hex[:8]
        self.name = "client-" + self.id
        self.data = {}
        self.rooms = set()
        self.connected = True
        self.authenticated = False
        self.connected_at = time.monotonic()

    @property
    def kind(self):
        return self.transport.kind

    @property
    def last_seen(self):
        return self.transport.reader.last_activity

    def send(self, event, data=_MISSING):
        """client.send(data) -> "message" event, or client.send("event", data)."""
        if data is _MISSING:
            event, data = "message", event
        return self._send_text(encode(event, data))

    def emit(self, event, data=None):
        return self._send_text(encode(event, data))

    def _send_text(self, text):
        if not self.connected:
            return False
        try:
            self.transport.send(text)
            return True
        except OSError:
            self.close()
            return False

    def join(self, room):
        return self.server.room(room).join(self)

    def leave(self, room):
        r = self.server._rooms.get(room)
        if r:
            r.leave(self)

    def close(self):
        self.connected = False
        self.transport.close()

    def __repr__(self):
        return f"<Client {self.id} {self.kind} {self.address[0]}>"


class Room:
    def __init__(self, server, name):
        self.server = server
        self.name = name
        self._members = {}

    @property
    def clients(self):
        with self.server._lock:
            return list(self._members.values())

    def join(self, client):
        with self.server._lock:
            self._members[client.id] = client
            client.rooms.add(self.name)
            self.server._rooms[self.name] = self
        return self

    def leave(self, client):
        with self.server._lock:
            self._members.pop(client.id, None)
            client.rooms.discard(self.name)
            if not self._members and self.server._rooms.get(self.name) is self:
                del self.server._rooms[self.name]

    def emit(self, event, data=None, exclude=None):
        text = encode(event, data)
        for c in self.clients:
            if exclude is None or c is not exclude:
                c._send_text(text)

    def __len__(self):
        return len(self._members)

    def __contains__(self, client):
        return client.id in self._members

    def __repr__(self):
        return f"<Room {self.name!r} ({len(self)})>"


class Server(Events):
    def __init__(self, port=5000, host="0.0.0.0", heartbeat=15, timeout=45,
                 sniff_timeout=0.5, auth_timeout=10, quiet=False):
        super().__init__()
        self.host, self.port = host, port
        self.heartbeat, self.timeout = heartbeat, timeout
        self.sniff_timeout, self.auth_timeout = sniff_timeout, auth_timeout
        self.quiet = quiet
        self.clients = {}
        self._rooms = {}
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._listener = None
        self._auth = None

    # public API
    def auth(self, fn):
        """@server.auth  def check(client, token): return True/False"""
        self._auth = (fn, arity(fn))
        return fn

    def room(self, name):
        with self._lock:
            return self._rooms.get(name) or Room(self, name)

    def broadcast(self, event, data=None, exclude=None):
        text = encode(event, data)
        with self._lock:
            targets = list(self.clients.values())
        for c in targets:
            if c.authenticated and (exclude is None or c is not exclude):
                c._send_text(text)

    def run(self):
        """Block and serve until Ctrl+C."""
        self._bind()
        self._log(f"flux listening on {self.host}:{self.port} (tcp + websocket)")
        threading.Thread(target=self._monitor, daemon=True).start()
        try:
            self._accept_loop()
        except KeyboardInterrupt:
            pass
        finally:
            self.stop()

    def start(self):
        """Serve in a background thread and return immediately."""
        self._bind()
        threading.Thread(target=self._monitor, daemon=True).start()
        threading.Thread(target=self._accept_loop, daemon=True).start()
        return self

    def stop(self):
        self._stop.set()
        if self._listener:
            try:
                self._listener.close()
            except OSError:
                pass
        for c in list(self.clients.values()):
            c.close()

    # internals
    def _log(self, *a):
        if not self.quiet:
            print(*a)

    def _bind(self):
        s = socket.socket()
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((self.host, self.port))
        s.listen(128)
        s.settimeout(0.5)
        self.port = s.getsockname()[1]
        self._listener = s
        self._stop.clear()

    def _accept_loop(self):
        while not self._stop.is_set():
            try:
                sock, addr = self._listener.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            threading.Thread(target=self._serve, args=(sock, addr), daemon=True).start()

    def _monitor(self):
        while not self._stop.wait(self.heartbeat):
            now = time.monotonic()
            for c in list(self.clients.values()):
                if now - c.last_seen > self.timeout:
                    c.close()
                elif not c.authenticated and now - c.connected_at > self.auth_timeout:
                    c.close()
                else:
                    try:
                        c.transport.ping()
                    except OSError:
                        c.close()

    def _negotiate(self, sock):
        reader = Reader(sock)
        sock.settimeout(self.sniff_timeout)
        try:
            first = sock.recv(4, socket.MSG_PEEK)
            if not first:
                raise ConnectionClosed()
        except socket.timeout:
            first = b""
        except OSError:
            raise ConnectionClosed()
        sock.settimeout(10)
        if first[:3] == b"GET":
            ws_server_handshake(sock, reader)
            sock.settimeout(None)
            return WSTransport(sock, reader)
        sock.settimeout(None)
        return TCPTransport(sock, reader)

    def _admit(self, client):
        client.authenticated = True
        client.emit("welcome", {"id": client.id})
        self._fire("connect", client)
        return True

    def _serve(self, sock, address):
        try:
            transport = self._negotiate(sock)
        except (ConnectionClosed, OSError):
            sock.close()
            return
        client = ServerClient(self, transport, address)
        with self._lock:
            self.clients[client.id] = client
        announced = False
        try:
            if self._auth is None:
                announced = self._admit(client)
            else:
                client.emit("auth_required")
            while True:
                event, data = decode(transport.recv())
                if event in ("__pong", "__hello"):
                    continue
                if not client.authenticated:
                    if event != "auth":
                        continue
                    fn, n = self._auth
                    ok = False
                    try:
                        ok = bool(fn(*(client, data)[:n]))
                    except Exception:
                        traceback.print_exc()
                    if not ok:
                        client.emit("auth_failed")
                        break
                    announced = self._admit(client)
                    continue
                self._fire("*", client, event, data)
                self._fire(event, client, data)
        except ConnectionClosed:
            pass
        except Exception:
            traceback.print_exc()
        finally:
            client.connected = False
            for r in list(client.rooms):
                client.leave(r)
            with self._lock:
                self.clients.pop(client.id, None)
            transport.close()
            if announced:
                self._fire("disconnect", client)
