# flux

Dead-simple sockets for Python. Events, rooms, auto-JSON, heartbeats, reconnection,
and one port that serves **both plain TCP and WebSocket (browser) clients**. No dependencies.

```python
from flux import Server

server = Server(5000)

@server.on("connect")
def connect(client): print(client.id, "connected")

@server.on("move")
def move(client, data):
    server.broadcast("moved", {"id": client.id, **data})

server.run()
```

```python
from flux import Client

client = Client("localhost", 5000)

@client.on("moved")
def moved(data): print(data)

client.emit("move", {"x": 100, "y": 50})   # queued until connected
client.run()                                # or client.start() for a background thread
```

Browser: `<script src="flux.js">` then `new Flux("ws://localhost:5000")` — same `.on` / `.emit`.

## API

**Server(port=5000, host="0.0.0.0", heartbeat=15, timeout=45)**
`.on(event)` · `.auth` · `.room(name)` · `.broadcast(event, data, exclude=None)` · `.run()` · `.start()` · `.stop()` · `.clients`

Special events: `connect`, `disconnect`, `message` (raw/non-envelope input, e.g. from `nc`), `*` (everything: `client, event, data`).
Handlers can take fewer arguments: `def join(client)` is fine.

**Server client:** `.id` `.name` `.data` `.rooms` `.kind` (`"tcp"`/`"websocket"`) · `.send(data)` / `.send(event, data)` / `.emit(event, data)` · `.join(room)` `.leave(room)` · `.close()`

**Room:** `.emit(event, data, exclude=None)` · `.join(c)` `.leave(c)` · `.clients` · `len(room)`

**Auth:** `@server.auth def check(client, token): return token == "..."` and `Client(..., token="...")`.
Clients that fail (or don't authenticate within 10s) are dropped; `connect` only fires after success.

**Client(host, port, token=None, reconnect=True)** — `.on` `.emit` `.run()` `.start()` `.wait_connected()` `.close()` `.id`

## Wire format
TCP: one JSON per line. WebSocket: one JSON per text frame. Shape: `{"event": "name", "data": ...}`.
Anything else arrives as a `message` event, so `nc localhost 5000` works for poking around.

## Not yet
UDP, asyncio API, Python-side WebSocket client, TLS. Handlers run on per-connection threads, so guard shared state with a lock.

## Try it
```
python -m unittest discover -s tests
python examples/cubes_server.py     # then open examples/cubes.html in two tabs
```
