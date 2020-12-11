import random
import sys
import threading
import time

from multiprocessing.connection import Client

from darlingkv.config import guiconfig
from darlingkv.config import netconfig
from darlingkv.common import ip
from darlingkv.common import ping
from darlingkv.common.binarysearch import binarySearch
from darlingkv.common.hash import intHash
from darlingkv.common.hash import strHash
from darlingkv.common.singleton import Singleton
from darlingkv.net import rpc
from darlingkv.net.endPoint import EndPoint


class RingNode(object):
    def __init__(self, address, storePort, communicatePort, token, state, timestamp = None):
        self.endpoint   = EndPoint(address, storePort, communicatePort)
        self.state      = state # states: active, inactive
        self.timestamp  = timestamp if timestamp else time.time()
        self.token      = token

    def serialize(self):
        s               = self.endpoint.serialize()
        s['state']      = self.state
        s['timestamp']  = self.timestamp
        s['token']      = self.token
        return s
    
    def hash(self):
        return intHash(self.token)

@Singleton
class HashRing:
    def __init__(self):
        self.ringSize       = netconfig.hashRingSize
        self.activeNodes    = [] # RingNode List
        self.totalNodes     = {} # Token -> RingNode

    def activeNodeNum(self):
        return len(self.activeNodes)

    def randomKNodes(self, k):
        k           = min(k, self.activeNodeNum())
        return random.choices(self.activeNodes, k=k)

    def sequentialKNodes(self, key, k):
        keyLoc      = strHash(key) % self.ringSize
        nodeIndex   = self.findTargetNode(keyLoc)

        k           = min(k, self.activeNodeNum())
        nodeIndices = [(nodeIndex + i)%self.activeNodeNum() for i in range(k)]

        return list(map(lambda index: self.activeNodes[index], nodeIndices))
    
    def serialize(self):
        return [v.serialize() for k, v in self.totalNodes.items()]

    def deserlize(self):
        # maybe a deserialize method is needed
        pass

    # This method is aborted
    def joinRing(self):
        localIp         = ip.localIp()
        storePort       = netconfig.storePort
        communicatePort = netconfig.communicatePort

        newEndPoint     = EndPoint(localIp, storePort, communicatePort)

    def update(self, serialNodes):
        for serialNode in serialNodes:
            address         = serialNode['address']
            communicatePort = serialNode['communicatePort']
            state           = serialNode['state']
            storePort       = serialNode['storePort']
            timestamp       = serialNode['timestamp']
            token           = serialNode['token']

            if token not in self.totalNodes:
                newNode = RingNode(address, storePort, 
                                   communicatePort, token, 
                                   state, timestamp)
            if state == 'active':
                self.addNode(newNode)
            elif state == 'inactive':
                self.removeNode(newNode)
            else:
                print("Invalide state:", state)

    # This function can only be called by seeds host
    # and allocate suitable token for the requester
    def insertInRing(self, serialEndPoint):
        token = hash(serialEndPoint) % self.ringSize
        while token in self.totalNodes:
            token = hash(token + random.randint(10)) % self.ringSize

        newNode = RingNode(serialEndPoint['address'],
                           serialEndPoint['storePort'],
                           serialEndPoint['communicatePort'],
                           token, 
                           'active')
        self.addNode(newNode)
        return newNode.serialize()
        
    def addNode(self, node):
        print("Add Node")
        if node.token not in self.totalNodes:
            self.totalNodes[node.token] = node
        else:
            tNode = self.totalNodes[node.token]
            if tNode.timestamp < node.timestamp:
                tNode.timestamp = node.timestamp
                if tNode.state == 'inactive':
                    tNode.state = 'active'
        
        tNode = self.totalNodes[node.token]
        if self.activeNodeNum() > 0:
            nodeIndex = self.findTargetNode(tNode.token)
            activeNode = self.activeNodes[nodeIndex]
            if activeNode.token != tNode.token:
                self.activeNodes.insert(nodeIndex, tNode)
        else:
            self.activeNodes.append(tNode)

    def removeNode(self, node):
        print("Remove Node")
        if node.token not in self.totalNodes:
            self.totalNodes[node.token] = node

        tNode = self.totalNodes[node.token]

        if tNode.timestamp < node.timestamp:
                tNode.timestamp = node.timestamp

                if tNode.state == 'active':
                    tNode.state = 'inactive'
                    nodeIndex   = self.findTargetNode(tNode.token)
                    self.activeNodes.pop(nodeIndex)
    
    def nodeScope(self, node):
        nodeIndex   = self.findTargetNode(node.token)
        preIndex    = (nodeIndex - 1 + self.activeNodeNum()) % self.activeNodeNum()

        return (self.activeNodes[preIndex].token, node.token)

    def offsetNode(self, node, offset):
        nodeIndex = self.findTargetNode(node.token)
        if self.activeNodes[nodeIndex].token != node.token:
            return None

        targetNodeIndex = (nodeIndex + offset + self.activeNodeNum()) % self.activeNodeNum()
        return self.activeNodes[targetNodeIndex]

    def findTargetNode(self, token):
        return binarySearch(self.activeNodes, token, lambda a, b: a.token < b)

    def show(self):
        print("Hash ring nodes:")
        print("Ring size is", self.activeNodeNum())
        hosts = self.serialize()
        for host in hosts:
            print(host)


# Below is an example of terrible design (¬‿¬)

# Process request for joining the hashring
# Can only be called by seed hosts
def registerRing(serialEndPoint):
    if ip.localHostName() not in guiconfig.seeds:
        # raise PermissionError("Seed Hosts Only!")
        print("This function can only be called by seed hosts")
        return

    ring = HashRing()
    serialNode = ring.insertInRing(serialNode)

    return serialNode

# send request to register host when bootstrap
def joinRing():
    for host in guiconfig.seeds:
        if ping.ping(host):
            serialEndPoint  = EndPoint(ip.localIp(),
                                      netconfig.storePort,
                                      netconfig.communicatePort)
            c           = Client(host, guiconfig.seeds[host])
            proxy       = rpc.RPCProxy(c)
            serialNode  = proxy.registerRing(serialEndPoint)

            ring        = HashRing()
            ring.update([serialNode])
            return
        
    raise RuntimeError("No seed hosts can be used!")

# join hashring when recovery
def rejoinRing():
    pass