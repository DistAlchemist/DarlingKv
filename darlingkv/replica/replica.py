

import darlingkv.config as conf
import darlingkv.net.hashring as hashring

from darlingkv.common.singleton import Singleton


# Replica is only responsible for the process of replicates data
# at the insert time without watching and processing the failure 
# of storage.


def replica(node):
    rep = Replica()
    pass

# replicate data by n copies
def UnAwareReplica(key, value, timestamp, n):
    ring            = hashring.HashRing()
    replicaNodes    = ring.sequentialKNodes(key, n)

    for node in replicaNodes:
        pass

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