import socket
import threading

from .events import Events
from .protocol import ConnectionClosed, Reader, TCPTransport, decode, encode


class Client(Events):
    """Python client. Emits made before the connection is up are queued and sent on connect."""

    def __init__(self, host="localhost", port=5000, token=None, reconnect=True,
                 reconnect_delay=1.0, max_delay=15.0, connect_timeout=10.0):
        super().__init__()
        self.host, self.port, self.token = host, port, token
        self.reconnect = reconnect
        self.reconnect_delay, self.max_delay = reconnect_delay, max_delay
        self.connect_timeout = connect_timeout
        self.id = None
        self.connected = False
        self._transport = None
        self._pending = []
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._ready = threading.Event()
        self._thread = None

    # public API 
    def emit(self, event, data=None):
        text = encode(event, data)
        with self._lock:
            if self.connected and self._transport:
                try:
                    self._transport.send(text)
                    return True
                except OSError:
                    pass
            if len(self._pending) < 1000:
                self._pending.append(text)
        return False

    def run(self):
        """Block, connecting (and reconnecting) until close() is called."""
        self._stop.clear()
        delay = self.reconnect_delay
        while not self._stop.is_set():
            try:
                sock = socket.create_connection((self.host, self.port), self.connect_timeout)
            except OSError as e:
                if not self.reconnect:
                    raise ConnectionError(f"could not connect to {self.host}:{self.port}") from e
                if self._stop.wait(delay):
                    break
                delay = min(delay * 2, self.max_delay)
                continue
            delay = self.reconnect_delay
            sock.settimeout(None)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            transport = TCPTransport(sock, Reader(sock))
            self._transport = transport
            try:
                transport.send(encode("__hello"))
                if self.token is not None:
                    transport.send(encode("auth", self.token))
                while True:
                    event, data = decode(transport.recv())
                    self._handle(event, data)
            except (ConnectionClosed, OSError):
                pass
            finally:
                with self._lock:
                    was, self.connected = self.connected, False
                    self._transport = None
                self._ready.clear()
                transport.close()
                if was:
                    self._fire("disconnect")
            if not self.reconnect or self._stop.wait(delay):
                break

    def start(self):
        """Run in a background thread."""
        self._thread = threading.Thread(target=self.run, daemon=True)
        self._thread.start()
        return self

    def wait_connected(self, timeout=None):
        return self._ready.wait(timeout)

    def close(self):
        self._stop.set()
        t = self._transport
        if t:
            t.close()

    # internals 
    def _handle(self, event, data):
        if event == "__ping":
            try:
                self._transport.send(encode("__pong"))
            except OSError:
                pass
            return
        if event == "welcome":
            self.id = data.get("id") if isinstance(data, dict) else None
            with self._lock:
                while self._pending:
                    self._transport.send(self._pending[0])
                    self._pending.pop(0)
                self.connected = True
            self._ready.set()
            self._fire("connect")
        elif event == "auth_failed":
            self._stop.set()
        self._fire("*", event, data)
        self._fire(event, data)
