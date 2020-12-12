import hashlib


def kvHash(data):
    hashValue = hashlib.md5(repr(data).encode('utf-8')).hexdigest()

    return int(hashValue, 16)

def strHash(s):
    hashValue = hashlib.md5(s.encode(encoding='utf-8')).hexdigest()
    return int(hashValue, 16)

def intHash(i):
    h = (i + 7983) << 8
    l = i & 2345345346241
    h = h ^ l + i
    h = h ^ (l + i)
    return h