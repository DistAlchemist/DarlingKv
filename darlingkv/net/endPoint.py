import os

from darlingkv.common.hash import intHash
from darlingkv.common.hash import strHash

class EndPoint:
    def __init__(self, address, storePort, communicatePort):
        self.address            = address
        self.storePort          = storePort
        self.communicatePort    = communicatePort

    def serialize(self):
        return {'address':          self.address, 
                'storePort':        self.storePort,
                'communicatePort':  self.communicatePort}

    def hash(self):
        return strHash(self.address) + intHash(self.port)