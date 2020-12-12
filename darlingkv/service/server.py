
import threading
import time

from multiprocessing.connection import Client

from darlingkv.config import netconfig
from darlingkv.common.hash import strHash
from darlingkv.net import hashring
from darlingkv.net import rpc
from darlingkv.replica import replica

class KvServer:
    def __init__(self):
        self.ring = hashring.HashRing()
        self.port = netconfig.kvPort

    def run(self):
        handler = rpc.RPCHandler()
        handler.register(self.processCMD)

        rpc.rpcServer(handler, ('localhost', self.port))

    def processCMD(self, msg):
        t = time.time()
        msg = msg.split(' ')
        
        cmd = msg[0]
        key = msg[1]

        replicaEndPoints = replica(key)

        # Ugly code
        if cmd == 'insert':
            value = msg[2]
            for endpoint in replicaEndPoints:
                c = Client((endpoint.address, endpoint.storePort))
                proxy = rpc.RPCProxy(c)
                return proxy.insert(key, value, t)
        elif cmd == 'get':
            for endpoint in replicaEndPoints:
                c = Client((endpoint.address, endpoint.storePort))
                proxy = rpc.RPCProxy(c)
                return proxy.get(key, t)
        elif cmd == 'delete':
            for endpoint in replicaEndPoints:
                c = Client((endpoint.address, endpoint.storePort))
                proxy = rpc.RPCProxy(c)
                return proxy.delete(key, t)
        elif cmd == 'exists':
            for endpoint in replicaEndPoints:
                c = Client((endpoint.address, endpoint.storePort))
                proxy = rpc.RPCProxy(c)
                return proxy.exists(key, t)
        else:
            return "Unkown cmd"


