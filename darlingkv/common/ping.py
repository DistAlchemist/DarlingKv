import os

def ping(hostname):
    return True if os.system("ping -c " + hostname) is 0 else False