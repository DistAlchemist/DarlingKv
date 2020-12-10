import pickle

from multiprocessing.connection import Listener
from threading import Thread


# RPC server
class RPCHandler:
    def __init__(self):
        self._functions = {}

    def register(self, func):
        self._functions[func.__name__] = func

    def handleConn(self, connection):
        try:
            while True:
                func_name, args, kwargs = pickle.loads(connection.recv())
                try:
                    r = self._functions[func_name](*args, **kwargs)
                    connection.send(pickle.dumps(r))
                except Exception as e:
                    connection.send(pickle.dumps(e))
        except EOFError:
            pass

def rpcServer(handler, address):
    sock = Listener(address=address)
    while True:
        client = sock.accept()
        t = Thread(target=handler.handleConn, args=(client,))
        t.daemon = True
        t.start()


# RPC client
class RPCProxy:
    def __init__(self, connection):
        self._connection = connection

    def __getattr__(self, name):
        def doRPC(*args, **kwargs):
            self._connection.send(pickle.dumps((name, args, kwargs)))
            result = pickle.loads(self._connection.recv())
            if isinstance(result, Exception):
                raise result
            return result
        return doRPC

