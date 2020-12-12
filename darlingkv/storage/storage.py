import os
import sys

from darlingkv.common.hash import strHash
from darlingkv.common.singleton import Singleton

class  StorageEngine:
    def __init__(self):
        self.memTable = {}

    def insert(self, key, value, timestamp):
        self.memTable[key] = {'value': value, 'timestamp': timestamp}

    def get(self, key, timestamp):
        return self.memTable[key]

    def delete(self, key, timestamp):
        del self.memTable[key]

    def exists(self, key, timestamp):
        return key in self.memTable

    def keys(self, timestamp):
        return list(self.memTable)

    def kvInScope(self, scope, timestamp):
        s, t = scope[0], scope[1]
        kv = [k, v for k, v in self.memTable if strHash(k) in range(s, t)]
        return kv

