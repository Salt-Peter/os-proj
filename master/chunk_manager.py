import random
import threading
import time
from collections import defaultdict
from threading import Lock
from typing import List, Dict, Set

from commons.errors import FileNotFoundErr, ChunkAlreadyExistsErr, ChunkhandleDoesNotExistErr, NoChunkServerAliveErr, \
    ChunkHandleNotFoundErr
from commons.loggers import default_logger
from commons.settings import REPLICATION_FACTOR, HEARTBEAT_INTERVAL
from commons.utils import pick_randomly, rpc_call

LEASE_TIMEOUT = 60  # expires in 1 minute


class Chunk:
    chunk_handle: int
    __slots__ = 'chunk_handle'

    def __init__(self, chunk_handle=0):
        self.chunk_handle = chunk_handle


# In-memory detailed information of a specific chunk.
class ChunkInfo:
    chunk_locations: List[str]
    chunk_handle: int
    __slots__ = 'chunk_handle', 'chunk_locations'

    def __init__(self, handle=0, locations=None):
        self.chunk_handle = handle  # Unique chunk handle.
        self.chunk_locations = locations

    def __repr__(self):
        return f'ChunkInfo(chunk_handle={self.chunk_handle}, chunk_locations={self.chunk_locations})'


class PathIndex:
    index: int
    path: str
    __slots__ = 'path', 'index'

    def __init__(self, path="", index=0):
        self.path = path
        self.index = index


class Lease:
    expiration: float
    primary: str
    __slots__ = 'primary', 'expiration'

    def __init__(self):
        self.primary = ''  # Primary chunk server's location.
        self.expiration = 0  # Lease expiration time.


class ChunkManager:
    lock: Lock
    chunk_handle: int
    chunks: Dict[str, Dict[int, Chunk]]
    handles: Dict[int, PathIndex]
    locations: Dict[int, ChunkInfo]
    active_chunk_servers: Set[str]
    leases: Dict[int, Lease]
    chunks_to_delete: List[int]
    chunks_of_chunk_server: Dict[str, List[int]]

    __slots__ = 'lock', 'chunk_handle', 'chunks', 'handles', 'locations', 'active_chunk_servers', \
                'leases', 'chunks_to_delete', 'chunks_of_chunk_server'

    def __init__(self):
        self.lock = threading.Lock()
        self.chunk_handle = 0  # incremented by 1 whenever a new chunk is created

        # (path, chunk index) -> chunk information (persistent)
        self.chunks = {}
        # chunk handle -> (path, chunk index) (inverse of chunks)
        self.handles = {}
        # chunk handle -> chunk locations (in-memory)
        self.locations = {}
        # a list if chunk servers
        self.active_chunk_servers = set()
        #  chunk handle -> lease
        self.leases = {}
        # a list of chunk handles to be deleted
        self.chunks_to_delete = []
        self.chunks_of_chunk_server = defaultdict(list)

    def __repr__(self):
        return f""" ChunkManager(chunk_handle={self.chunk_handle},
                                 chunks={self.chunks},
                                 handles={self.handles},
                                 locations={self.locations},
                                 chunk_servers={self.active_chunk_servers}
                                 leases={self.leases})"""

    def find_locations(self, path, chunk_index):
        with self.lock:
            chunk_locations, chunk_handle, err = self.get_chunk_info(path, chunk_index)
            return chunk_locations, chunk_handle, err

    # Assumes lock is acquired
    # Get chunk information associated with a file and a chunk index.
    # Returns chunk information and errors.
    def get_chunk_info(self, path, chunk_index):
        value = self.chunks.get(path, None)
        if not value:
            log.debug(FileNotFoundErr)
            return None, None, FileNotFoundErr

        chunk = value.get(chunk_index, None)
        if not chunk:
            log.debug("Chunk index not found.")
            return None, None, "Chunk index not found."

        chunk_info = self.locations.get(chunk.chunk_handle, None)
        if not chunk_info:
            log.debug("Locations not found.")
            return None, None, "Locations not found"

        return chunk_info.chunk_locations, chunk_info.chunk_handle, None

    def add_chunk(self, path, chunk_index):
        with self.lock:
            return self.add_chunk_helper(path, chunk_index)

    # Assumes lock is acquired
    def add_chunk_helper(self, path, chunk_index):
        chunk = self.chunks.get(path, None)
        if not chunk:
            self.chunks[path] = {}

        chunk_info = self.chunks[path].get(chunk_index, None)
        if chunk_info:
            log.debug("Chunk index already exists.")
            return chunk_info, ChunkAlreadyExistsErr

        # get a unique chunk handle
        handle = self.chunk_handle

        # increment for future
        self.chunk_handle += 1

        locations = pick_randomly(self.active_chunk_servers, REPLICATION_FACTOR)

        # update our dicts
        self.chunks[path][chunk_index] = Chunk(handle)
        self.locations[handle] = ChunkInfo(handle, locations)
        self.handles[handle] = PathIndex(path, chunk_index)

        return self.locations[handle], None

    # Find lease holder and return its location.
    def find_lease_holder(self, chunk_handle):
        with self.lock:
            ok = self.check_lease(chunk_handle)
            # If no lease holder, then grant a new lease.
            if not ok:
                err = self.add_lease(chunk_handle)
                if err:
                    return Lease(), err

            # Return current lease holder for handle.
            lease = self.leases[chunk_handle]
            return lease, None

    # Pre-condition: m.lock is acquired.
    # will check whether the lease is still valid.
    def check_lease(self, chunk_handle):
        lease = self.leases.get(chunk_handle, None)
        if not lease:
            return False

        # If lease on the primary has already expired, return false
        if lease.expiration < time.time():
            return False

        return True

    # Assumes m.lock is acquired.
    # will grant a lease to a randomly selected server as the primary.
    # returns err if any or None
    def add_lease(self, chunk_handle):
        chunk_info = self.locations.get(chunk_handle, None)

        if not chunk_info:
            return ChunkhandleDoesNotExistErr

        lease = self.leases.get(chunk_handle, None)

        if not lease:
            # Entry not found, create a new one.
            lease = Lease()
            self.leases[chunk_handle] = lease

        #  If no chunk server is alive, can't grant a new lease.
        if len(chunk_info.chunk_locations) == 0:
            return NoChunkServerAliveErr

        # Assign new values to lease.
        # pick primary randomly
        lease.primary = chunk_info.chunk_locations[
            random.randint(1,
                           min(len(chunk_info.chunk_locations), REPLICATION_FACTOR)) - 1]  # -1 for zero based indexing

        lease.expiration = time.time() + LEASE_TIMEOUT
        self.leases[chunk_handle] = lease

        return None

    # // Get (file, chunk index) associated with the specified chunk handle.
    def get_path_index_from_handle(self, chunk_handle):
        with self.lock:  # Fixme : might need an rlock here
            path_index = self.handles.get(chunk_handle, None)
            if not path_index:
                return None, ChunkHandleNotFoundErr
            return path_index, None

    # // Set the location associated with a chunk handle.
    def set_chunk_location(self, chunk_handle, address):
        with self.lock:
            info = self.locations.get(chunk_handle, None)
            if not info:
                info = ChunkInfo(chunk_handle, [])
                self.locations[chunk_handle] = info

            # Add address into the locations array.
            # Need to ensure the there are no duplicates in the array.
            if address not in info.chunk_locations:
                info.chunk_locations.append(address)
            self.chunks_of_chunk_server[address].append(chunk_handle)

    def poll_chunkservers(self):
        """A one time polling function, runs when master is started to get list of chunks from active chunk servers
            and update the chunks_of_chunkserver dict."""
        log.debug("****Polling active chunkservers start***")
        for chunk_server in self.active_chunk_servers:
            log.debug("Polling chunkserver %s", chunk_server)
            cs = rpc_call(chunk_server)
            try:
                chunk_handles = cs.get_chunk_handles()
                # update chunks_of_chunkserver dict
                self.chunks_of_chunk_server[chunk_server] = chunk_handles
                log.debug("Polling complete for chunkserver: %s", chunk_server)
            except ConnectionRefusedError:
                log.error("Polling failed for chunkserver: %s", chunk_server)

        log.debug("****Polling active chunkservers end***")

    def update_chunkserver_list(self, chunksrv_addr):
        self.active_chunk_servers.add(chunksrv_addr)

    # // delete all chunk handles related to given path.
    # // and them into chunks_to_delete[]
    def update_deletechunk_list(self, path):
        chunk_dict = self.chunks.get(path, None)
        if chunk_dict:
            for chunk_index, chunk in chunk_dict.items():
                self.chunks_to_delete.append(int(chunk.chunk_handle))
            del self.chunks[path]

    def test_connection(self, chunk_server_addr):
        chunk_server = rpc_call(chunk_server_addr)
        try:
            # try to connect with chunkserver
            resp = chunk_server.delete_bad_chunk(self.chunks_to_delete)
            if resp:
                log.info("%s has deleted all bad chunks", chunk_server_addr)
                return True
            else:
                log.info("%s is unable to delete all bad chunk handle", chunk_server_addr)
                return True
        except ConnectionRefusedError:
            log.info("Unable to connect with %s", chunk_server_addr)
            return False

    def beat(self):
        # FIXME: Simplify
        while True:
            time.sleep(HEARTBEAT_INTERVAL)
            log.debug("Heart Beating %s", self.locations)
            log.debug("Heart Beating %s", self.active_chunk_servers)

            # build list of dead chunk servers
            # by testing a connection to them
            dead_chunk_servers = [cs for cs in self.active_chunk_servers if not self.test_connection(cs)]

            log.debug("Dead chunk servers list = %s", dead_chunk_servers)

            # delete dead chunk server from active chunk servers list
            self.active_chunk_servers.difference_update(dead_chunk_servers)

            # loop over all chunk handles of dead chunk server
            for dead_chunk_server in dead_chunk_servers:
                # get list of chunks that need to be replicated
                chunk_handles = self.chunks_of_chunk_server.get(dead_chunk_server, [])

                for chunk_handle in chunk_handles:
                    chunk_info = self.locations.get(chunk_handle, None)

                    if chunk_info and dead_chunk_server in chunk_info.chunk_locations:
                        # remove dead chunkserver from chunk's chunk_info.chunk_locations
                        chunk_info.chunk_locations.remove(dead_chunk_server)

                        dest_cs = None
                        # if replication is needed
                        # and we have enough number of active chunkservers
                        # then perform replication
                        if REPLICATION_FACTOR - len(chunk_info.chunk_locations) > 0 \
                                and len(
                            self.active_chunk_servers) >= REPLICATION_FACTOR:  # TODO: Probably handle with semaphore

                            while True:
                                # keep looping until we pick a chunk server which does not already contain this chunk
                                # TODO: don't run infinitely, set a fixed max number of times this is executed
                                rand_loc = pick_randomly(self.active_chunk_servers, 1)[0]
                                if rand_loc not in chunk_info.chunk_locations:
                                    dest_cs = rand_loc
                                    break

                        if not dest_cs:
                            # if no valid destination chunkserver found, skip this chunks replication
                            continue

                        # else perform replication
                        # call order_chunk copy_from_peer
                        peer_address = pick_randomly(chunk_info.chunk_locations, 1)[0]
                        cs_proxy = rpc_call(dest_cs)

                        try:
                            err = cs_proxy.order_chunk_copy_from_peer(peer_address, chunk_handle)
                            if err:
                                log.info("Unable to replicate to %s due to %s", dest_cs, err)
                        except ConnectionRefusedError:
                            log.info("Unable to connect to %s for %s replication", dest_cs, chunk_handle)

                # delete dead_chunk_server from chunks_of_chunk_server_list
                # TODO: donot remove if replication was not performed
                self.chunks_of_chunk_server.pop(dead_chunk_server, None)


log = default_logger
