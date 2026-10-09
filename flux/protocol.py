import base64
import hashlib
import json
import socket
import struct
import threading
import time

GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
MAX_MESSAGE = 1 << 20  # 1 MiB


class ConnectionClosed(Exception):
    pass


def encode(event, data=None):
    return json.dumps(
        {"event": event, "data": data}, separators=(",", ":"), default=str
    )


def decode(text):
    try:
        obj = json.loads(text)
    except ValueError:
        return "message", text
    if isinstance(obj, dict) and isinstance(obj.get("event"), str):
        return obj["event"], obj.get("data")
    return "message", obj


class Reader:
    def __init__(self, sock):
        self.sock = sock
        self.buf = bytearray()
        self.last_activity = time.monotonic()

    def _fill(self):
        try:
            chunk = self.sock.recv(65536)
        except OSError as e:
            raise ConnectionClosed(str(e))
        if not chunk:
            raise ConnectionClosed()
        self.last_activity = time.monotonic()
        self.buf += chunk

    def read_exact(self, n):
        while len(self.buf) < n:
            self._fill()
        out = bytes(self.buf[:n])
        del self.buf[:n]
        return out

    def read_until(self, delim, limit=MAX_MESSAGE):
        while True:
            i = self.buf.find(delim)
            if i != -1:
                out = bytes(self.buf[:i])
                del self.buf[: i + len(delim)]
                return out
            if len(self.buf) > limit:
                raise ConnectionClosed("message too large")
            self._fill()


class _Transport:
    def __init__(self, sock, reader):
        self.sock = sock
        self.reader = reader
        self._wlock = threading.Lock()

    def _write(self, data):
        with self._wlock:
            self.sock.sendall(data)

    def close(self):
        for fn in (lambda: self.sock.shutdown(socket.SHUT_RDWR), self.sock.close):
            try:
                fn()
            except OSError:
                pass


class TCPTransport(_Transport):
    kind = "tcp"

    def recv(self):
        while True:
            line = self.reader.read_until(b"\n").strip()
            if line:
                return line.decode("utf-8", "replace")

    def send(self, text):
        self._write(text.encode() + b"\n")

    def ping(self):
        self.send(encode("__ping"))


def _unmask(data, mask):
    if not data:
        return data
    m = (mask * (len(data) // 4 + 1))[: len(data)]
    return (int.from_bytes(data, "big") ^ int.from_bytes(m, "big")).to_bytes(
        len(data), "big"
    )


class WSTransport(_Transport):
    kind = "websocket"

    @staticmethod
    def _frame(opcode, payload=b""):
        n = len(payload)
        head = bytes([0x80 | opcode])
        if n < 126:
            head += bytes([n])
        elif n < 65536:
            head += b"\x7e" + struct.pack(">H", n)
        else:
            head += b"\x7f" + struct.pack(">Q", n)
        return head + payload

    def send(self, text):
        self._write(self._frame(1, text.encode()))

    def ping(self):
        self._write(self._frame(9))

    def recv(self):
        r = self.reader
        message = bytearray()
        while True:
            b1, b2 = r.read_exact(2)
            fin, op = b1 & 0x80, b1 & 0x0F
            n = b2 & 0x7F
            if n == 126:
                n = struct.unpack(">H", r.read_exact(2))[0]
            elif n == 127:
                n = struct.unpack(">Q", r.read_exact(8))[0]
            if n > MAX_MESSAGE:
                raise ConnectionClosed("frame too large")
            mask = r.read_exact(4) if b2 & 0x80 else None
            payload = r.read_exact(n)
            if mask:
                payload = _unmask(payload, mask)
            if op == 8:
                try:
                    self._write(self._frame(8, payload[:2]))
                except OSError:
                    pass
                raise ConnectionClosed()
            if op == 9:
                self._write(self._frame(10, payload))
                continue
            if op == 10:
                continue
            message += payload
            if len(message) > MAX_MESSAGE:
                raise ConnectionClosed("message too large")
            if fin:
                return message.decode("utf-8", "replace")


def ws_server_handshake(sock, reader):
    head = reader.read_until(b"\r\n\r\n", 16384).decode("latin-1")
    headers = {}
    for line in head.split("\r\n")[1:]:
        if ":" in line:
            k, v = line.split(":", 1)
            headers[k.strip().lower()] = v.strip()
    key = headers.get("sec-websocket-key")
    if not key:
        sock.sendall(
            b"HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\nWebSocket only."
        )
        raise ConnectionClosed("not a websocket request")
    accept = base64.b64encode(hashlib.sha1((key + GUID).encode()).digest()).decode()
    sock.sendall(
        (
            "HTTP/1.1 101 Switching Protocols\r\n"
            "Upgrade: websocket\r\nConnection: Upgrade\r\n"
            f"Sec-WebSocket-Accept: {accept}\r\n\r\n"
        ).encode()
    )
