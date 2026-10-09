"""Multiplayer cubes. Run this, then open cubes.html in two browser tabs."""

import random
import threading
from flux import Server

server = Server(5000)
players = {}
lock = threading.Lock()


def clamp(v, lo, hi):
    return max(lo, min(hi, float(v)))


@server.on("connect")
def connect(client):
    with lock:
        players[client.id] = {
            "x": random.randint(50, 550),
            "y": random.randint(50, 350),
            "color": f"hsl({random.randint(0, 360)},70%,55%)",
        }
        server.broadcast("players", players)


@server.on("move")
def move(client, data):
    with lock:
        p = players.get(client.id)
        if p:
            p["x"], p["y"] = clamp(data["x"], 0, 580), clamp(data["y"], 0, 380)
            server.broadcast("players", players)


@server.on("disconnect")
def disconnect(client):
    with lock:
        players.pop(client.id, None)
        server.broadcast("players", players)


server.run()
