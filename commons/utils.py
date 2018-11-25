import xmlrpc.client

from cachetools import TTLCache


# Helper functions
def rpc_call(server_addr):
    return xmlrpc.client.ServerProxy(server_addr,
                                     verbose=False,
                                     allow_none=True)


# randomly picks n random elements from given sequence
def pick_randomly(seq, n):
    import random
    return random.sample(seq,
                         min(len(seq), n))


def get_cache(timeout=60, maxsize=10):
    return TTLCache(maxsize=maxsize, ttl=timeout)


def ensure_dir(dir_path):
    import os
    os.makedirs(dir_path, exist_ok=True)
