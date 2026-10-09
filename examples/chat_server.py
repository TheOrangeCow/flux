from flux import Server

server = Server(5000)


@server.on("join")
def join(client, room):
    client.join(room)
    server.room(room).emit(
        "chat", {"user": "server", "text": f"{client.id} joined {room}"}
    )


@server.on("chat")
def chat(client, data):
    for room in client.rooms:
        server.room(room).emit("chat", {"user": client.id, "text": data})


server.run()
