
from multiprocessing.connection import Client

import darlingkv.common.ping as ping
import darlingkv.net.hashring as hashring
import darlingkv.net.rpc as rpc

from darlingkv.common.singleton import Singleton
from darlingkv.config import repconfig

# Hard for me to decide whether OOP is needed

# Replica is only responsible for the process of replicates data
# at the insert time without watching and processing the failure 
# of storage.
def replica(key, value, timestamp, n):
    if repconfig.strategy == 'Rack Unaware':
        rackUnAwareReplica(key, value, timestamp, n)
    elif repconfig.strategy == 'Rack Aware':
        rackAwareReplica(key, value, timestamp, n)
    else:
        raise UnboundLocalError("Unkown Strategy")

# replicate data by n copies
def rackUnAwareReplica(key, value, timestamp, n):
    ring            = hashring.HashRing()
    replicaNodes    = ring.sequentialKNodes(key, n)

    for node in replicaNodes:
        c = Client((node.address, node.storePort))
        proxy = rpc.RPCProxy(c)
        proxy.insert(key, value, timestamp)

def rackAwareReplica(key, value, timestamp, n):
    pass

################################################################
@Singleton
class Replica:
    def __init__(self):
        self.replicaNum = config.replicaNum
        self.ring       = hashring.HashRing()

        if config.strategy == 'Rack UnAware':
            self.strategy = self.RackAware
        elif config.strategy == 'Rack Aware':
            self.strategy = self.RackUnaware
        else:
            raise UnboundLocalError("Unkown replica strategy!")

    def __call__(self, endPoint):
        pass

    def replicate(self, key, value):
        nodeIndex = self.ring.findTargetNode()

    def RackUnaware(self):
        pass

    def RackAware(self):
        pass