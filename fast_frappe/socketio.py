import socketio


def connect_socketIO(url: str = 'http://localhost:9000'):
	sio = socketio.Client()
	sio.connect(url)
	return sio


sio = connect_socketIO()
