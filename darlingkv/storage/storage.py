import os
import sys

from darlingkv.common.singleton import Singleton

class  StorageEngine:
    def __init__(self):
        self.memTable = {}

    def insert(self, key, value, timestamp):
        self.memTable[key] = {'value': value, 'timestamp': timestamp}

    def get(self, key):
        return self.memTable[key]

    def delete(self, key):
        del self.memTable[key]

    def exists(self, key):
        return key in self.memTable

    def keys(self):
        return list(self.memTable)

    def allKeyValue(self):
        return self.memTable

