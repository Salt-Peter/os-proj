import threading
from typing import Dict, List
from xmlrpc.server import SimpleXMLRPCServer

from commons.datastructures import ChunkInfo
from commons.errors import FileNotFoundErr
from commons.loggers import request_logger
from commons.settings import DEFAULT_MASTER_ADDR, DEFAULT_IP, CHUNK_SIZE
from commons.utils import rpc_call, ensure_dir


class ChunkServer:
    chunks: Dict[int, ChunkInfo]
    pending_extensions: List[int]
    data: Dict[str, List[bytes]]

    __slots__ = 'my_addr', 'master_addr', 'metadata_file', 'path', 'chunks', 'mutex', \
                'pending_extensions', 'pendingextensions_lock', 'data', 'data_mutex'

    def __init__(self, my_addr, master_addr, path):
        self.my_addr = my_addr
        self.master_addr = master_addr

        # Filename of a file that contains chunkserver's meta data
        self.metadata_file = f"chunkserver{my_addr}"
        self.path = path
        # Store a mapping from handle to information.
        self.chunks = {}
        self.mutex = threading.Lock()
        # Stores pending lease extension requests on chunkhandles.
        self.pending_extensions = []
        #  Only used by Hearbeat and ChunkServer.add_chunkextension_request.
        #  Possible to have ChunkServer.mutex held first, therefore do not acquire
        #  ChunkServer.mutex after acquire this lock.
        self.pendingextensions_lock = threading.Lock()

        # Stores client's data in memory before commit to disk.
        self.data = {}
        self.data_mutex = threading.Lock()

        # tell master about the presence of this chunk server
        ms = rpc_call(self.master_addr)
        ms.notify_master(self.my_addr)

    # PushData handles client RPC to store data in memory.
    # Data is identified with a mapping from DataId:[ClientID, Timestamp] -> Data.
    def push_data(self, client_id, timestamp, data):
        log.debug("me=%s: client_id=%s, timestamp=%s, data=%s", self.my_addr, client_id, timestamp, data)
        with self.data_mutex:
            key = f'{client_id}|{timestamp}'
            value = self.data.get(key, None)

            # if data already exists
            if value:
                return
            # else
            self.data[key] = data

    # Write handles client RPC write requests to the primary chunk. The primary
    # first applies requested write to its local storage, serializes and records
    # the order of application in ChunkServer.writeRequests, then sends the write
    # requests to secondary replicas.
    def write(self, client_id, timestamp, path, chunk_index, chunk_handle, offset, chunk_locations):
        log.debug("ChunkServer addr: %s", self.my_addr)
        with self.mutex:
            log.debug("ChunkServer: Write RPC. Lock Acquired")
            # Extract/define arguments.
            key = f'{client_id}|{timestamp}'
            data = self.data.get(key, None)
            if not data:
                log.debug("ChunkServer: Write RPC. Lock Released.")
                return "ChunkServer.Write: requested data is not in memory"
            length = len(data)
            filename = f"{chunk_handle}"

            # Apply write request to local state.
            err = self.apply_write(filename, data, offset)
            if err:
                log.debug("ChunkServer: Write RPC. Lock Released.")
                return err
            else:
                del self.data[key]

            #   // Update chunkserver metadata.
            self.report_chunk_info(chunk_handle, chunk_index, path, length, offset)

        # Apply the write to all secondary replicas.
        # lock automatically release outside context
        err = self.apply_to_secondary(client_id, timestamp, path, chunk_index, chunk_handle, offset, chunk_locations)
        if err:
            log.debug("ChunkServer: Write RPC. Lock Released.")
            return err

        with self.mutex:
            #   // Since we are still writing to the chunk, we must continue request
            #   // lease extensions on the chunk.
            # TODO: probably need to request for chunk lease extension
            log.debug("ChunkServer: Write RPC. Lock Released.")
            return None

    # // applyWrite is a helper function for Write and SerializedWrite to apply
    # // writes from memory to local storage.
    # // Note: ChunkServer.mutex must be held before calling this function.
    def apply_write(self, filename, data, offset):
        # Open file that stores the chunk.
        # FIXME: possible bug, 'w' will truncate existing file
        try:
            with open(f'{self.path}/{filename}', 'w') as fp:  # TODO: create with 0777 perm
                fp.seek(offset)
                fp.write(data)
        except FileNotFoundError:
            return FileNotFoundErr

    # // reportChunkInfo is a helper function for ChunkServer.Write,
    # // ChunkServer.SerializedWrite and ChunkServer.Append to update chunkserver's
    # // metadata after a write request.
    def report_chunk_info(self, chunk_handle, chunk_index, path, length, offset):
        #   // Update chunkserver metadata.
        ok = self.chunks.get(chunk_handle, None)

        #  // If we have never seen this chunk before,
        #  // or chunk size has changed, we should
        #  // report to Master immediately.

        if not ok:  # we have never seen this chunk
            self.chunks[chunk_handle] = ChunkInfo(path, chunk_handle, chunk_index)  # length will default to zero

        chunk_info = self.chunks[chunk_handle]

        if offset + length > chunk_info.length:  # chunk size has changed
            chunk_info.length = offset + length
            report_chunk(self, chunk_info)

    # // apply_to_secondary is used by the primary replica to apply any modifications
    # // that are serialized by the replica, to all of its secondary replicas.
    def apply_to_secondary(self, client_id, timestamp, path, chunk_index, chunk_handle, offset, chunk_locations):
        #   // RPC each secondary chunkserver to apply the write.
        for address in chunk_locations:
            if address != self.my_addr:
                cs = rpc_call(address)
                err = cs.serialized_write(client_id, timestamp, path, chunk_index, chunk_handle, offset,
                                          chunk_locations, False)
                if err:
                    return err
        return None

    # // serialized_write handles RPC calls from primary replica's write requests to
    # // secondary replicas.
    def serialized_write(self, client_id, timestamp, path, chunk_index, chunk_handle, offset, chunk_locations,
                         append_mode):
        log.debug(self.my_addr)
        with self.mutex:
            key = f'{client_id}|{timestamp}'
            data = None
            if append_mode:
                # TODO: Do some stuff needed in append
                # # // Padding chunk with zeros. TODO: but Why?
                # pad_length = CHUNK_SIZE - offset
                # # data = [0] * pad_length   # TODO: how to pad data
                pass
            else:
                # // Fetch data from chunk_server.data
                data = self.data.get(key)
                if not data:
                    return "ChunkServer.SerializedWrite: requested data is not in memory"

            #   // Apply write reqeust to local state.
            filename = f'{chunk_handle}'
            err = self.apply_write(filename, data, offset)
            if err:
                return err
            elif not append_mode:
                del self.data[key]

            #   // Update chunkserver metadata.
            length = len(data)
            self.report_chunk_info(chunk_handle, chunk_index, path,
                                   length, offset)
            return None

    # read content from specific chunk
    def read(self, chunk_handle, offset, length):
        """Called by client to read data from specific chunk"""
        # open file to read data
        log.debug("CHUNK SERVER READ CALLED")
        try:
            with open(f'{self.path}/{chunk_handle}', 'rb') as file:
                file.seek(int(offset))  # goes to specific offset in a chunk
                filecontent = file.read(length)  # read all required content in filecontent
                log.debug("FileContent %s", filecontent)
                return filecontent, None
        except Exception as err:
            return None, err

    # // Append accepts client append request and append the data to an offset
    # // chosen by the primary replica. It then serializes the request just like
    # // Write request, and send it to all secondary replicas.
    # // If appending the record to the current chunk would cause the chunk to
    # // exceed the maximum size, append fails and the client must retry.
    # // It also puts the offset chosen in AppendReply so the client knows where
    # // the data is appended to.
    def append(self, client_id, timestamp, chunk_handle, chunk_index, path, chunk_locations):
        log.debug("ChunkServer addr: %s", self.my_addr)
        with self.mutex:
            log.debug("ChunkServer: Append RPC. Lock Acquired")
            # Extract/define arguments.
            key = f'{client_id}|{timestamp}'
            data = self.data.get(key, None)
            if not data:
                log.debug("ChunkServer: Append RPC. Lock Released.")
                return "ChunkServer.Append: requested data is not in memory"
            length = len(data)
            filename = f"{chunk_handle}"

            # Get length of the current chunk so we can calculate an offset.
            chunk_info = self.chunks[chunk_handle]
            # If we cannot find chunkInfo, means this is a new chunk, therefore offset
            # should be zero, otherwise the offset should be the chunk length.
            if chunk_info is None:
                chunk_length = 0
            else:
                chunk_length = chunk_info.length

            # If appending the record to the current chunk would cause the chunk to
            # exceed the maximum size, report error for client to retry at another
            # chunk.
            if chunk_length + length >= CHUNK_SIZE:
                # TODO error by padding chunk
                return -1

            # Apply write request to local state, with chunkLength as offset.
            err = self.apply_append(filename, data, chunk_length)
            if err:
                log.debug("ChunkServer: Append RPC. Lock Released.")
                return -1

            # Update chunkserver metadata.
            self.report_chunk_info(chunk_handle, chunk_index, path, length, chunk_length)

            # Apply append to all secondary replicas.
            err = self.apply_to_secondary(client_id, timestamp, path, chunk_index, chunk_handle, chunk_length,
                                          chunk_locations)
            if err:
                return -1

            # TODO chunk lease extension

            return chunk_length + (chunk_index * CHUNK_SIZE)

    def apply_append(self, filename, data, offset):
        # Open file that stores the chunk.
        # FIXME: possible bug, 'w' will truncate existing file
        try:
            with open(f'{self.path}/{filename}', 'a') as fp:  # TODO: create with 0777 perm
                fp.seek(offset)
                fp.write(data)
        except FileNotFoundError:
            return FileNotFoundErr

    def order_chunk_copy_from_peer(self, peer_address, chunk_handle):
        """This RPC is called by master to order a chunkserver to copy some chunks from a peer chunk server
        so as to meet the replication goal for that chunk."""
        peer_chunk_server = rpc_call(peer_address)
        # get chunk_info from peer
        chunk_index, path, length = peer_chunk_server.get_chunk_info_from_peer(chunk_handle)
        # get chunk's actual data
        data, err = peer_chunk_server.read(chunk_handle, 0, length)
        if err:
            log.error(err)
            return err

        # write data with that chunk_handle as filename to local filesystem
        filename = f"{chunk_handle}"

        err = self.apply_write(filename, data.data.decode('utf-8'), 0)
        if err:
            return err

        self.report_chunk_info(chunk_handle, chunk_index, path, length, 0)

    def get_chunk_info_from_peer(self, chunk_handle):
        """Called by a chunkserver for another chunkserver to get a chunk's data"""
        chunk_info = self.chunks.get(chunk_handle, None)
        return chunk_info.chunk_index, chunk_info.path, chunk_info.length


def report_chunk(cs, chunk_info):
    ms = rpc_call(cs.master_addr)

    # TODO: receive returned error if any
    ms.report_chunk(cs.my_addr, chunk_info.chunk_handle, chunk_info.chunk_index, chunk_info.length, chunk_info.path)


def start_chunkserver(master_addr, my_ip, my_port, path):
    ensure_dir(path)  # make sure this path exists

    my_address = f'http://{my_ip}:{my_port}'
    cs = ChunkServer(my_address, master_addr, path)

    chunk_server = SimpleXMLRPCServer((my_ip, my_port),
                                      logRequests=True,
                                      allow_none=True)

    chunk_server.register_introspection_functions()
    chunk_server.register_instance(cs)
    chunk_server.serve_forever()

    # TODO: launch heart beat on separate thread


if __name__ == '__main__':
    log = request_logger

    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--ip', default=DEFAULT_IP)
    parser.add_argument('--port', type=int, required=True)
    parser.add_argument('--master', default=DEFAULT_MASTER_ADDR, help="http://<ip address>:<port>")
    parser.add_argument('--path', help="Defaults to temp/ck<PORT>")
    args = parser.parse_args()

    start_chunkserver(args.master, args.ip, args.port, args.path or f"temp/ck{args.port}")
