import base64, os, socket, sys, threading, unittest, json

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from flux import Server, Client


def ws_connect(port):
    s = socket.create_connection(("127.0.0.1", port))
    key = base64.b64encode(os.urandom(16)).decode()
    s.sendall(
        (
            "GET / HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n"
        ).encode()
    )
    buf = b""
    while b"\r\n\r\n" not in buf:
        buf += s.recv(1)
    assert b" 101 " in buf
    s.settimeout(3)
    return s


def ws_send(s, text):
    p = text.encode()
    m = os.urandom(4)
    s.sendall(
        bytes([0x81, 0x80 | len(p)]) + m + bytes(b ^ m[i % 4] for i, b in enumerate(p))
    )


def ws_recv(s):
    b1, b2 = s.recv(2)
    n = b2 & 0x7F
    data = b""
    while len(data) < n:
        data += s.recv(n - len(data))
    return json.loads(data)


class T(unittest.TestCase):
    def setUp(self):
        self.server = Server(0, "127.0.0.1", quiet=True).start()
        self.port = self.server.port

    def tearDown(self):
        self.server.stop()

    def test_echo_and_events(self):
        s = self.server
        log = []
        s.on("connect", lambda c: log.append("connect"))
        s.on("disconnect", lambda c: log.append("disconnect"))

        @s.on("message")
        def m(client, data):
            client.send(data)

        got = threading.Event()
        box = []
        c = Client("127.0.0.1", self.port, reconnect=False)

        @c.on("message")
        def r(data):
            box.append(data)
            got.set()

        c.emit("message", "hi")
        c.start()
        self.assertTrue(got.wait(3))
        self.assertEqual(box, ["hi"])
        c.close()
        for _ in range(50):
            if "disconnect" in log:
                break
            threading.Event().wait(0.05)
        self.assertEqual(log, ["connect", "disconnect"])

    def test_rooms_and_broadcast(self):
        s = self.server

        @s.on("join")
        def join(client, data):
            client.join(data)

        @s.on("say")
        def say(client, data):
            s.room(data["room"]).emit("said", data["text"], exclude=client)

        a, b, c = [Client("127.0.0.1", self.port, reconnect=False) for _ in range(3)]
        seen = {"b": threading.Event(), "c": threading.Event()}
        b.on("said", lambda d: seen["b"].set())
        c.on("said", lambda d: seen["c"].set())
        for x in (a, b, c):
            x.start()
            x.wait_connected(3)
        a.emit("join", "r1")
        b.emit("join", "r1")
        threading.Event().wait(0.3)
        self.assertEqual(len(s.room("r1")), 2)
        a.emit("say", {"room": "r1", "text": "yo"})
        self.assertTrue(seen["b"].wait(3))
        self.assertFalse(seen["c"].wait(0.3))
        for x in (a, b, c):
            x.close()

    def test_auth(self):
        @self.server.auth
        def check(client, token):
            return token == "secret"

        ok = Client("127.0.0.1", self.port, token="secret", reconnect=False).start()
        self.assertTrue(ok.wait_connected(3))
        bad = Client("127.0.0.1", self.port, token="nope", reconnect=False).start()
        self.assertFalse(bad.wait_connected(1))
        ok.close()

    def test_raw_tcp_text(self):
        self.server.on("message", lambda c, d: c.send("echo:" + d))
        s = socket.create_connection(("127.0.0.1", self.port))
        s.sendall(b"hello\n")
        f = s.makefile()
        self.assertEqual(json.loads(f.readline())["event"], "welcome")
        self.assertEqual(json.loads(f.readline())["data"], "echo:hello")

    def test_websocket(self):
        @self.server.on("move")
        def move(client, data):
            self.server.broadcast("players", {client.id: data})

        w = ws_connect(self.port)
        self.assertEqual(ws_recv(w)["event"], "welcome")
        ws_send(w, json.dumps({"event": "move", "data": {"x": 1, "y": 2}}))
        msg = ws_recv(w)
        self.assertEqual(msg["event"], "players")
        self.assertEqual(list(msg["data"].values()), [{"x": 1, "y": 2}])

    def test_python_and_ws_mixed(self):
        self.server.on("chat", lambda c, d: self.server.broadcast("chat", d))
        w = ws_connect(self.port)
        ws_recv(w)
        got = threading.Event()
        box = []
        c = Client("127.0.0.1", self.port, reconnect=False)
        c.on("chat", lambda d: (box.append(d), got.set()))
        c.start()
        c.wait_connected(3)
        ws_send(w, json.dumps({"event": "chat", "data": "from browser"}))
        self.assertTrue(got.wait(3))
        self.assertEqual(box, ["from browser"])
        c.close()


if __name__ == "__main__":
    unittest.main()
