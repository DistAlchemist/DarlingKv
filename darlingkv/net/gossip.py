import socket
import threading
import time

from multiprocessing.connection import Client

from darlingkv.common.ping import ping
from darlingkv.config import netconfig
from darlingkv.failure import failure
from darlingkv.net.hashring import HashRing
from darlingkv.net import rpc


# Gossip exchanges host state information
# TODO: Exchange commit log
class Gossip:
    def __init__(self, port):
        self.ring   = HashRing()
        self.port   = port
        self.config = netconfig

    def distributeInfo(self):
        return self.ring.serialize()

    def deamon(self):
        ringNodes = self.ring.randomKNodes(self.config.gossipNum)
        for ringNode in ringNodes:
            if not ping(ringNode.endpoint.address):
                # process host failure
                failure.failureHandle(ringNode)
            serialNodes = self.pull(ringNode.endpoint)

            self.mergeInfo(serialNodes)

    def pull(self, endpoint):
        address = (endpoint.address, self.port)
        c       = Client(address)
        proxy   = rpc.RPCProxy(c)

        return proxy.distributeInfo()

    def mergeInfo(self, serialNodes):
        self.ring.update(serialNodes)
