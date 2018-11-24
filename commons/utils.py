import xmlrpc.client


# Helper functions
def rpc_call(server_addr):
    return xmlrpc.client.ServerProxy(server_addr,
                                     verbose=False,
                                     allow_none=True)


# randomly picks n random elements from array arr
def pick_randomly(arr, n):
    res = []
    import random
    perm = range(len(arr))
    random.shuffle(perm)

    for i in range(n):
        res.append(arr[perm[i]])

    return res
