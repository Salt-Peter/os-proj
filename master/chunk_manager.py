import threading
import time
from threading import Lock
from typing import List, Dict

from numpy import uint64, uintp


# Persistent information of a specific chunk.
class Chunk:
    chunk_handle: uintp
    __slots__ = 'chunk_handle'

    def __init__(self):
        self.chunk_handle = uint64(0)


# In-memory detailed information of a specific chunk.
class ChunkInfo:
    locations: List[str]
    handle: uintp
    __slots__ = 'handle', 'locations'

    def __init__(self):
        self.handle = uint64(0)  # Unique chunk handle.
        self.locations = []


class PathIndex:
    index: uintp
    path: str
    __slots__ = 'path', 'index'

    def __init__(self):
        self.path = ""
        self.index = uint64(0)


class Lease:
    expiration: float
    primary: str
    __slots__ = 'primary', 'expiration'

    def __init__(self):
        self.primary = ''  # Primary chunk server's location.
        self.expiration = time.time()  # Lease expiration time.


class ChunkManager:
    mutex: Lock
    chunk_handle: uintp
    chunks: Dict[str, Dict[uintp, Chunk]]
    handles: Dict[uintp, PathIndex]
    locations: Dict[uintp, ChunkInfo]
    chunk_servers: List[str]
    leases: Dict[uintp, Lease]

    __slots__ = 'lock', 'chunk_handle', 'chunks', 'handles', 'locations', 'chunk_servers', 'leases'

    def __init__(self):
        self.lock = threading.Lock()
        self.chunk_handle = uint64(0)  # incremented by 1 whenever a new chunk is created

        # (path, chunk index) -> chunk information (persistent)
        self.chunks = {}
        # chunk handle -> (path, chunk index) (inverse of chunks)
        self.handles = {}
        # chunk handle -> chunk locations (in-memory)
        self.locations = {}
        # a list if chunk servers
        self.chunk_servers = []
        #  chunk handle -> lease
        self.leases = {}

    def find_locations(self, path, chunk_index):
        with self.lock:
            return self.get_chunk_info(path, chunk_index)

    # Assumes lock is acquired
    # Get chunk information associated with a file and a chunk index.
    # Returns chunk information and errors.
    def get_chunk_info(self, path, chunk_index):
        value = self.chunks.get(path, None)
        # if:
