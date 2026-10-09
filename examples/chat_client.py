from flux import Client

client = Client("localhost", 5000)


@client.on("chat")
def chat(data):
    print(f"[{data['user']}] {data['text']}")


client.emit("join", "lobby")
client.start()

while True:
    client.emit("chat", input())
