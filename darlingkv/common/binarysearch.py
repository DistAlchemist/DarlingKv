

def binarySearch(arr, val, cmp):
    if len(arr) == 0:
        return None

    l, h = 0, len(arr)
    while l < h:
        mid = (l + h) // 2
        if cmp(arr[mid], val) < 0:
            l = mid + 1
        elif cmp(arr[mid], val) > 0:
            h = mid
        else:
            return mid
    return h
