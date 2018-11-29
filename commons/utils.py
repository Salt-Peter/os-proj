import xmlrpc.client

from cachetools import TTLCache


# Helper functions
def rpc_call(server_addr):
    """A wrapper around the actual xmlrpc call to specified server address."""
    return xmlrpc.client.ServerProxy(server_addr,
                                     verbose=False,
                                     allow_none=True)


def pick_randomly(seq, n):
    """Randomly picks n elements from a given SEQuence and returns a LIST of picked elements."""
    import random
    return random.sample(seq,
                         min(len(seq), n))


def get_cache(timeout=60, maxsize=10):
    """Returns a TTL cache. Used to cache the response of certain APIs for some time."""
    return TTLCache(maxsize=maxsize, ttl=timeout)


def ensure_dir(dir_path):
    """Ensures that the directory specified by dir_path exists by creating it if it does not exist."""
    import os
    os.makedirs(dir_path, exist_ok=True)
