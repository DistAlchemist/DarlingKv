
from darlingkv.config import guiconfig
from darlingkv.net import hashring

# Check if the system is initialized the first time 
# 
def bootstrap():
    # Get token
    hashring.joinRing()
