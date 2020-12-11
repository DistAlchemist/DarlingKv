import sys

from darlingkv.common.singleton import Singleton

DataBasePath = '/root/db/'
ConfigFile = 'config/config'
LogPath = 'log/'

@Singleton
class GUIConfig:
    def __init__(self, dbpath = DataBasePath):
        self.dbpath     = dbpath
        self.configFile = self.dbpath + ConfigFile
        self.logPath    = self.dbpath + LogPath

        self.seeds      = {'localhost': 8989}

        self.hostToken  = 123

    def LoadConfig(self):
        with open(self.configFile) as f:
            pass

@Singleton
class ReplicaConfig:
    def __init__(self, replicapath = None):
        self.configPath = replicapath

        # replica strategy
        self.strategy   = 'Rack Unaware'
        self.replicaNum = 3

@Singleton
class NetConfig:
    def __init__(self, configPath = None):
        # file path
        self.configPath         = configPath

        # HashRing
        self.hashRingSize       = 1024
        self.heartBeatIntv      = 1

        # Gossip
        self.gossipNum          = 3

        # Port
        self.gossipPort         = 9989
        self.ringPort           = 9990 # aborted
        self.communicatePort    = 9992
        self.storePort          = 9991


guiconfig = GUIConfig()
netconfig = NetConfig()
repconfig = ReplicaConfig()