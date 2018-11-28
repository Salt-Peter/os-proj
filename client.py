import random
import time

from commons.datastructures import DataId
from commons.errors import ChunkAlreadyExistsErr
from commons.loggers import default_logger
from commons.settings import DEFAULT_MASTER_ADDR, CHUNK_SIZE, REPLICATION_FACTOR, APPEND_SIZE
from commons.utils import rpc_call
# data structure for client
from master.chunk_manager import ChunkInfo


class Client:
    # Read: https://docs.python.org/3/reference/datamodel.html#slots
    __slots__ = 'client_id', 'master_addr', 'location_cache', 'lease_holder_cache'

    def __init__(self, master_addr):
        self.master_addr = master_addr

        master_server = rpc_call(self.master_addr)
        # call master to get a unique client id
        self.client_id = master_server.unique_client_id()

        self.location_cache = {}  # TODO: implement cache with timeout. need some kind of expiring dict
        self.lease_holder_cache = {}  # TODO: implement cache with timeout

    def __repr__(self):
        return f'Client(client_id={self.client_id!r}, master_addr={self.master_addr!r})'

    # create a file
    def create(self, path):
        master_server = rpc_call(self.master_addr)
        resp, err = master_server.create(path)
        if resp:
            log.debug("Create API response %s", resp)
        else:
            log.error("Error creating file '%s'. Why? : %s", path, err)

    def write(self, path, offset, data):
        length = len(data)
        # eg: len(data)=11; offset=5; CHUNK_SIZE=3
        # start_chunk_idx = 5 // 3 = 1
        # end_chunk_idx = (5 + 11 - 1) // 3 = 5
        # both start and end are inclusive
        start_chunk_idx = offset // CHUNK_SIZE
        end_chunk_idx = (offset + length - 1) // CHUNK_SIZE  # inclusive

        start_idx = 0

        for i in range(start_chunk_idx, end_chunk_idx + 1):  # +1 bcoz range end is non inclusive
            start_offset = 0
            end_offset = CHUNK_SIZE  # exclusive

            if i == start_chunk_idx:
                # special case for the first chunk to be written to
                # eg: offset = 5 ; CHUNK_SIZE=3; end_chunk_idx=5
                # start_offset = 5%3 = 2; start_chunk_idx = 5//3 = 1
                #
                # Chunk0|Chunk1|Chunk2|Chunk3|Chunk4|Chunk5
                # ------------------------------------------
                # |0|1|2::0|1|2::0|1|2::0|1|2::0|1|2::0|1|2|
                # ------------------------------------------
                #             ^ <-- start offset        ^ <-- end offset(exclusive)
                start_offset = offset % CHUNK_SIZE

            if i == end_chunk_idx:
                # special case for the last chunk to be written
                # eg offset = 5; CHUNK_SIZE=3; length=11
                # rem = (5+11) % 3 = 1
                rem = (offset + length) % CHUNK_SIZE  # whether the last chunk will be completely filled or not
                end_offset = rem if rem != 0 else CHUNK_SIZE

            ok = self.write_helper(path, i, start_offset, end_offset,
                                   data[start_idx:start_idx + end_offset - start_offset])
            if not ok:
                return False

            start_idx += end_offset - start_offset

        return True

    # Returns True if write is successful else false
    def write_helper(self, path, chunk_index, start, end, data):
        chunk_handle, chunk_locations, err = self.get_chunk_guaranteed(path, chunk_index)

        if err:
            return False

        data_id = DataId(self.client_id, time.time())

        # Push data to all replicas' memory.
        err = self.push_data(chunk_locations, data_id, data)
        if err:
            log.error('Data not pushed to all replicas.')
            return False

        # Once data is pushed to all replicas, send write request to the primary replica.
        # primary = address of primary chunk server
        primary = self.find_lease_holder(chunk_handle)

        if not primary:
            log.error("Primary chunk server not found.")
            return False

        primary_cs = rpc_call(primary)
        err = primary_cs.write(data_id.client_id, data_id.timestamp, path,
                               chunk_index, chunk_handle, start,
                               chunk_locations)

        if err:
            return False

        return True

    # The get_chunk_details takes in a path name and a chunkIndex and
    # guarantees to return a chunkHandle and chunkLocations.
    # It first looks for chunk locations,
    # if not found, it adds the chunk to master server.
    def get_chunk_guaranteed(self, path, chunk_index):
        chunk_handle, chunk_locations, err = self.find_chunk(path, chunk_index)

        if err:
            # cannot find the chunk, add the chunk
            chunk_handle, chunk_locations, err = self.add_chunk(path, chunk_index)

        if err and err == ChunkAlreadyExistsErr:
            # some other client must have added the chunk simultaneously
            # highly unlikely but possible
            chunk_handle, chunk_locations, err = self.find_chunk(path, chunk_index)

        return chunk_handle, chunk_locations, err

    # find chunk handle and locations given filename and chunk index
    def find_chunk(self, path, chunk_index):
        key = f'{path}:{chunk_index}'
        value = self.location_cache.get(key, None)
        if value:
            # cached value found
            return value.chunk_handle, value.chunk_locations, None

        # else: not found in cache, get from master server
        ms = rpc_call(self.master_addr)
        chunk_locations, chunk_handle, err = ms.find_locations(path, chunk_index)

        if not err:
            # Save into location cache
            chunk_info = ChunkInfo(chunk_handle, chunk_locations)
            self.location_cache[key] = chunk_info
            return chunk_handle, chunk_locations, err

        return None, None, err

    # returns chunk_handle and chunklocations of the newly added chunk
    def add_chunk(self, path, chunk_index):
        ms = rpc_call(self.master_addr)
        chunk_handle, chunk_locations, err = ms.add_chunk(path, chunk_index)

        return chunk_handle, chunk_locations, err

    # find_lease_holder returns the address of current lease holder(one of the chunk servers) of the target chunk.
    def find_lease_holder(self, chunk_handle):
        key = f'{chunk_handle}'
        value = self.lease_holder_cache.get(key)
        if value:
            return value['primary']

        # If not found in cache, RPC the master server.
        ms = rpc_call(self.master_addr)
        primary, lease_ends, err = ms.find_lease_holder(chunk_handle)

        if not err:
            self.lease_holder_cache[key] = {'primary': primary, 'lease_ends': lease_ends}
            return primary

        return None

    # The push_data function pushes data to all replica's memory through RPC.
    def push_data(self, chunk_locations, data_id, data):
        for srv_addr in chunk_locations:
            cs = rpc_call(srv_addr)
            err = cs.push_data(data_id.client_id, data_id.timestamp, data)
            if err:
                return err

        return None

    # get FileLength
    def getfilelength(self, path):
        """This function calls Master Server GetFileLength Function to
        get total length of the file"""
        master_server = rpc_call(self.master_addr)
        filelength, err = master_server.get_file_length(path)
        log.debug("%s length is: %s", path, filelength)

        return filelength, err

    # read a file
    def read(self, path, byteoffset, bytestoread, filename):
        """This function will take filename,byteoffset and bytestoread
        from user and will return the file."""
        filelength, err = self.getfilelength(path)
        if err:
            log.debug("Error while fetching file length %s", err)
        else:
            log.debug("File length fetched from server %s", filelength)
        if bytestoread < 0:
            lastbytetoread = filelength
        else:
            lastbytetoread = min(byteoffset + bytestoread, filelength)
        startchunkindex = byteoffset // CHUNK_SIZE
        endchunkindex = (lastbytetoread - 1) // CHUNK_SIZE
        with open(filename, "ab") as file:
            for i in range(startchunkindex, endchunkindex + 1):
                startoffset = 0
                length = CHUNK_SIZE
                if i == startchunkindex:
                    startoffset = byteoffset % CHUNK_SIZE
                if i == endchunkindex:
                    rem = lastbytetoread % CHUNK_SIZE
                    if rem == 0:
                        length = CHUNK_SIZE
                    else:
                        length = rem
                chunkdata, err = self.read_helper(path, i, startoffset, length)
                if err:
                    log.debug("Unable to read required data, Error while reading chunkindex %s: %s", i, err)
                    return err
                else:
                    log.debug("ChunkData Received: %s, type %s", chunkdata, type(chunkdata))
                    file.write(chunkdata.data)
        return None

    # read a chunk
    def read_helper(self, path, chunk_index, start, length):
        """Call Chunkserver RPC to read chunkdata"""
        chunk_handle, chunk_locations, err = self.find_chunk(path, chunk_index)
        if err:
            return None, err
        random_num = random.randint(1, min(len(chunk_locations), REPLICATION_FACTOR)) - 1  # -1 for zero based index
        chunk_loc = chunk_locations[random_num]
        log.debug("Chunk Handle  %s and chunk Locations %s ", chunk_handle, chunk_locations)
        chunk_server = rpc_call(chunk_loc)
        data, err = chunk_server.read(chunk_handle, start, length)
        # TODO :Handle case if server is down
        return data, err

    # create a dir
    def create_dir(self, path):
        master_server = rpc_call(self.master_addr)
        resp, err = master_server.create_dir(path)
        if resp:
            log.debug("Create Dir API response %s", resp)
        else:
            log.error("Error creating file '%s'. Why? : %s", path, err)

    # list all files in a directory
    def list_allfiles(self, path):
        master_server = rpc_call(self.master_addr)
        resp, err = master_server.list_allfiles(path)
        if resp:
            log.debug("List of files in %s:\n", path)
            for file in resp:
                log.debug("%s\n", file)
        else:
            log.error("Error creating file '%s'. Why? : %s", path, err)

    # remove file
    def delete(self, path):
        master_server = rpc_call(self.master_addr)
        err = master_server.delete(path)
        if err:
            log.error("Error while deleting %s : %s", path, err)
        else:
            log.info("File Deleted Successfully")

    # // Append writes data to an offset chosen by the primary chunk server.
    # // Data is only appended if its size if less then AppendSize, which is one
    # // fourth of ChunkSize.
    # // Returns (offset chosen by primary, nil) if success, appropriate
    # // error otherwise.
    # // The caller must check return error before using the offset.
    def append(self, path, data):
        length = len(data)
        # First check if the size is valid.
        if length > APPEND_SIZE:
            log.error("ERROR: Data size exceeds append limit.")
            return -1

        # To calculate chunkIndex we must get the length.
        filelength, err = self.getfilelength(path)
        if err:
            log.error("Error while fetching file length %s", err)
        else:
            log.debug("File length fetched from server %s", filelength)

        chunk_index = filelength // CHUNK_SIZE

        # Get chunkHandle and chunkLocations
        chunk_handle, chunk_locations, err = self.get_chunk_guaranteed(path, chunk_index)
        print("APPEND :: ", chunk_handle, chunk_locations, err)
        if err:
            return -1

        # Construct dataId with clientId and current timestamp.
        data_id = DataId(self.client_id, time.time())

        # Push data to all replicas' memory.
        err = self.push_data(chunk_locations, data_id, data)
        if err:
            log.error('Data not pushed to all replicas.')
            return -1

        # Once data is pushed to all replicas, send append request to the primary.
        primary = self.find_lease_holder(chunk_handle)

        if not primary:
            log.error("Primary chunk server not found.")
            return -1

        # Make Append call to primary chunk server
        primary_cs = rpc_call(primary)
        offset = primary_cs.append(data_id.client_id, data_id.timestamp,
                                   chunk_handle, chunk_index, path,
                                   chunk_locations)

        return offset


if __name__ == "__main__":
    log = default_logger

    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--master', default=DEFAULT_MASTER_ADDR, help="http://<ip address>:<port>")
    args = parser.parse_args()

    client = Client(args.master)

    log.info("Client: %s", client)

    client.create('a')
    client.write('a', 0, "A man a plan canal panama.")
    # client.write('a', 0, "Alpha Omega")
    client.read('a', 0, -1, "temp/content")
    client.create('b')
    client.write('b', 0, "OS Project- Google File System.")
    # client.write('a', 0, "Alpha Omega")
    client.read('b', 0, -1, "temp/content1")
    # append_offset = client.append('a', "l")
    # client.read('a', 0, -1, "temp/content1")
    # print("APPEND OFFSET : ", append_offset)
    # if append_offset == -1:
    #     print("Error!!")

    client.delete('a')
