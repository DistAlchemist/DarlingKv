import time

from multiprocessing.connection import Client

from darlingkv.config import repconfig
from darlingkv.net import hashring
from darlingkv.net import rpc


# Simple failure detection and handle strategy is taken.
def failureHandle(failNode):
    ring            = hashring.HashRing()
    scope           = ring.nodeScope(failNode)
    targetNode      = ring.offsetNode(failNode, repconfig.replicaNum - 1)

    # Remove failNode from ring
    failNode.timestamp = time.time() # This increase congnitive load
    ring.removeNode(failNode)


    targetAddress   = targetNode.endpoint.address
    targetStorePort = targetNode.endpoint.storePort
    c               = Client((targetAddress, targetStorePort))
    proxy           = rpc.RPCProxy(c)
    proxy.migrate(targetNode.token, failNode, scope)


