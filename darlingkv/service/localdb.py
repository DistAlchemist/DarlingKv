

from darlingkv.config import netconfig
from darlingkv.net import rpc
from darlingkv.storage.storage import StorageEngine



class StorageServer:
    def __init__(self, port):
        self.db         = None
        self.handler    = None
        self.port       = port

    def run(self):
        self.db = StorageEngine()
        
        self.handler = rpc.RPCHandler()
        self.registerFunction()

        rpc.rpcServer(self.handler, ('localhost', self.port))

    def registerFunction(self):
        self.handler.register(self.db.allKeyValue)
        self.handler.register(self.db.delete)
        self.handler.register(self.db.exists)
        self.handler.register(self.db.get)
        self.handler.register(self.db.insert)
        self.handler.register(self.db.keys)

# Aborted
def storageServer(storePort):
    database = StorageEngine()
    handler  = rpc.RPCHandler()

    # register functions
    registerFunction(handler, database)

    # run server
    rpc.rpcServer(handler, ('localhost', storePort))