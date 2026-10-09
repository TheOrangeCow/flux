from flux import Server

server = Server(5000)


@server.on("connect")
def connect(client):
    print(client.name, "connected")


@server.on("message")
def message(client, data):
    print(client.name, data)
    client.send(data)


@server.on("disconnect")
def disconnect(client):
    print(client.name, "left")


server.run()
