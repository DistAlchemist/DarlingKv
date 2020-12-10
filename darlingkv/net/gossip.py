import socket
import threading
import time

from multiprocessing.connection import Client

from darlingkv.common.ping import ping
from darlingkv.config import netconfig
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
        data = {'activeHosts': self.ring.nodeList, 'downHosts': self.ring.otherNodes}
        return data

    def deamon(self):
        endpoints = self.ring.randomKNodes(self.config.gossipNum)
        for endpoint in endpoints:
            if not ping(node.address):
                # process host failure
                self.ring.removeNode(endpoint)
            nodeInfo = self.pull(endpoint)

            self.mergeInfo(nodeInfo)

    def pull(self, endpoint):
        address = (endpoint.address, self.port)
        c       = Client(address)
        proxy   = rpc.RPCProxy(address)

        return proxy.distributeInfo()

    def mergeInfo(self, nodeInfo):
        activeHosts = nodeInfo['activeHosts']
        downHosts   = nodeInfo['downHosts']
        self.ring.updateNodes(activeHosts, downHosts)
