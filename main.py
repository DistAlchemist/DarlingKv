import threading

import darlingkv.config as config
from darlingkv.service import bootstrap
from darlingkv.service import communicate
from darlingkv.service import localdb
from darlingkv.service import server


def main():
    bootstrap.bootstrap()
    db          = localdb.StorageServer()
    commSever   = communicate.CommunicateServer()

    db_t = threading.Thread(target=db.run, args=(config.netconfig.storePort))
    db_t.start()

    commSever_t = threading.Thread(target=commSever.run, args=(config.netconfig.communicatePort))
    commSever_t.start()

    server.KvServer()
    server.run()

if __name__ == "__main__":
    main()