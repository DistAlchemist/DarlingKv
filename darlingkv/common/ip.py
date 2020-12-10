import socket


def localHostName():
    return socket.gethostname()

def localIp():
    return socket.gethostbyname(localHostName())
