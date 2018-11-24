import xmlrpc.client

from cachetools import TTLCache


# Helper functions
def rpc_call(server_addr):
    return xmlrpc.client.ServerProxy(server_addr,
                                     verbose=False,
                                     allow_none=True)


# randomly picks n random elements from array arr
def pick_randomly(arr, n):
    # TODO: implement random sampling methods like reservoir sampling
    res = []
    import random
    perm = list(range(len(arr)))
    random.shuffle(perm)

    for i in range(min(n, len(arr))):
        res.append(arr[perm[i]])

    return res


def get_cache(timeout=60, maxsize=10):
    return TTLCache(maxsize=maxsize, ttl=timeout)
