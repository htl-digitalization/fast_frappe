import asyncio
import time
from fastapi import FastAPI, WebSocket
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import json
app = FastAPI()
# Add the CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

html = """
<!DOCTYPE html>
<html>
    <head>
        <title>Chat</title>
    </head>
    <body>
        <h1>WebSocket Chat</h1>
        <form action="" onsubmit="sendMessage(event)">
            <input type="text" id="messageText" autocomplete="off"/>
            <button>Send</button>
        </form>
        <ul id='messages'>
        </ul>
        <script>
            var ws = new WebSocket("ws://localhost:8888/ws");
            ws.onmessage = function(event) {
                var messages = document.getElementById('messages')
                var message = document.createElement('li')
                var content = document.createTextNode(event.data)
                message.appendChild(content)
                messages.appendChild(message)
            };
            function sendMessage(event) {
                var input = document.getElementById("messageText")
                ws.send(input.value)
                input.value = ''
                event.preventDefault()
            }
        </script>
    </body>
</html>
"""


@app.get("/")
async def get():
    return HTMLResponse(html)

# @app.websocket("/ws")
# async def llama_index_ws_response():

# @app.websocket("/ws")
# async def websocket_endpoint(websocket: WebSocket):
#     await websocket.accept()
#     while True:
#         data = await websocket.receive_text()
#         data_json = json.loads(data)
#         message = data_json.get("message")
#         response = {"received_message": message}
#         for i in range(3):
#             await websocket.send_text(json.dumps(response))
#             time.sleep(1)          

def data_generator():
    # counter = 0
    for i in range(3):
        # counter += 1
        yield f"Data {i}"

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()

    async def send_data_periodically(generator):
        for data in generator:
            response = {"streamed_data": data}
            await websocket.send_text(json.dumps(response))
            await asyncio.sleep(0.5)  # Send data every 1 second

    async def receive_data():
        while True:
            data = await websocket.receive_text()
            data_json = json.loads(data)
            message = data_json.get("message")
            response = {"received_message": message}
            await websocket.send_text(json.dumps(response))

    generator = data_generator()
    await asyncio.gather(send_data_periodically(generator), receive_data())

# @app.websocket("/ws")
# async def websocket_endpoint(websocket: WebSocket):
#     await websocket.accept()
#     while True:
#         data = await websocket.receive_text()
#         await websocket.send_text(f"Message text was: {data}")