import threading
import time

from darlingkv.config import netconfig
from darlingkv.net import gossip
from darlingkv.net import rpc
from darlingkv.net import hashring



class CommunicateServer:
    def __init__(self, port):
        self.gossip     = gossip.Gossip(port)
        self.handler    = rpc.RPCHandler()
        self.port       = port
        self._running   = True

    def run(self):
        self.registerFunction()

        pulltask = threading.Thread(target=self.exchangeInfo, daemon=True)
        waittask = threading.Thread(target=self.waitRequest, daemon=True)
        pulltask.start()
        waittask.start()

    def registerFunction(self):
        self.handler.register(self.gossip.distributeInfo)
        self.handler.register(hashring.registerRing)
    
    def waitRequest(self):
        rpc.rpcServer(self.handler, ('localhost', self.port))
        print("CommunicateServer is waiting for contact...")

    def exchangeInfo(self):
        print("Start to exchange data between neighbors")
        while self._running:
            self.gossip.deamon()
            time.sleep(1)

    def terminate(self):
        self._running = False
