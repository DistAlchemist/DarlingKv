
from multiprocessing.connection import Client

from darlingkv.net import rpc
from darlingkv.net import hashring
from darlingkv.service import localdb


# @localToken: indicate which db we need to migrate data to
def migrate(localToken, failsNode, scope):
    ring        = hashring.HashRing()

    localNode   = ring.findTargetNode(localToken)
    if localNode.token == localToken:
        failNode        = ring.findTargetNode(failsNode['token'])

        if failNode.token < failsNode['timestamp']:
            preNode     = ring.offsetNode(failNode, -1)
            migrateDataFromHost(localNode, preNode, scope)

            nextNode    = ring.offsetNode(failNode, 1)
            migrateDataFromHost(localNode, nextNode, scope)

            # update ring state
            ring.removeNode(failNode)
            failNode.timestamp  = failsNode['timestamp']
    else:
        print("Can't find migrate target: ", localToken)

def migrateDataFromHost(localNode, srcNode, scope):
    records = fetchDataFromHost(ringNode.endpoint, scope)

    c = Client(('localhost', localNode.endpoint.storePort))
    proxy = rpc.RPCProxy(c)

    for key, item in records.items():
        proxy.insert(key, item['value'], item['timestamp'])
    

def fetchDataFromHost(endpoint, scope):
    c       = Client((endpoint['address'], endpoint['storePort']))
    proxy   = rpc.RPCProxy(c)

    return proxy.kvInScope(scope)

