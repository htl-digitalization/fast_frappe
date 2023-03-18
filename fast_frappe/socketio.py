import socketio


async def connect_socketIO(url: str = 'http://localhost:9000'):
	sio = socketio.AsyncClient()
	await sio.connect(url)
	return sio

sio = connect_socketIO()
