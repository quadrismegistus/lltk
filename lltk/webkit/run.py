#!bin/python
from webkit import app,socketio
#app.run(debug=True,threaded=True)
socketio.run(app,port=1789,debug=True)
