import time

from commons.errors import ChunkAlreadyExistsErr
from commons.loggers import Logger
from commons.settings import MASTER_ADDR, CHUNK_SIZE
from commons.utils import rpc_call


# data structure for client
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
            logging.debug("Create API response %s", resp)
        else:
            logging.error("Error creating file '%s'. Why? : %s", path, err)

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

        data_id = {
            'client_id': self.client_id,
            'timestamp': time.time()
        }

        # Push data to all replicas' memory.
        err = self.push_data(chunk_locations, data_id, data)
        if err:
            logging.error('Data not pushed to all replicas.')
            return False

        # Once data is pushed to all replicas, send write request to the primary replica.
        # primary = address of primary chunk server
        primary = self.find_lease_holder(chunk_handle)

        if not primary:
            logging.error("Primary chunk server not found.")
            return False

        primary_cs = rpc_call(primary)
        err = primary_cs.write(data_id=data_id, path=path, chunk_index=chunk_index, chunk_handle=chunk_handle,
                               offset=start, chunk_locations=chunk_locations)

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
        resp, err = ms.find_locations(path, chunk_index)

        if not err:
            # Save into location cache
            self.location_cache[key] = resp

        return resp.chunk_handle, resp.chunk_locations, err

    # returns chunk_handle and chunklocations of the newly added chunk
    def add_chunk(self, path, chunk_index):
        ms = rpc_call(self.master_addr)
        resp, err = ms.add_chunk(path, chunk_index)

        return resp.chunk_handle, resp.chunk_locations, err

    # find_lease_holder returns the address of current lease holder(one of the chunk servers) of the target chunk.
    def find_lease_holder(self, chunk_handle):
        key = f'{chunk_handle}'
        value = self.lease_holder_cache.get(key)
        if value:
            return value.primary

        # If not found in cache, RPC the master server.
        ms = rpc_call(self.master_addr)
        resp, err = ms.find_lease_holder(chunk_handle)

        if not err:
            self.lease_holder_cache[key] = resp
            return resp.primary

        return None

    def push_data(self, chunk_locations, data_id, data):
        for srv_addr in chunk_locations:
            cs = rpc_call(srv_addr)
            err = cs.push_data(data_id, data)
            if err:
                return err

        return None


# get FileLength
def getfileLength(path, c):
    """This function calls Master Server GetFileLength Function to
    get total length of the file"""
    master_server = rpc_call(c.master_addr)
    filelength, err = master_server.get_filelength(path)
    if filelength:
        logging.debug("Length of File %s returned by server %s",path,filelength)
        return filelength
    else:
        logging.error("Error while getting filelength from Master: %s", err)


# read a file
def readfile(path, byteoffset, bytestoread, c):
    """This function will take filename,byteoffset and bytestoread
    from user and will return the file."""
    filelength = getfileLength(path, c)
    lastbytetoread = min(byteoffset+bytestoread,filelength)
    startchunkindex = byteoffset//CHUNK_SIZE
    endchunkindex = lastbytetoread//CHUNK_SIZE
    finaldata = []
    for i in range(startchunkindex,endchunkindex+1):
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
        chunkdata, err = readchunk(path,i,startoffset,length,c)
        if chunkdata:
            finaldata.append(chunkdata)
        else:
            logging.error("Error while reading chunkindex %s: %s",i,err)


# read a chunk
def readchunk(path,chunk_index,start,length,c):
    """Call Chunkserver RPC to read chunkdata"""
    chunk_handle, chunk_locations, err = findchunklocation(path, chunk_index)
    if err:
        return None,err
    






if __name__ == "__main__":
    client = Client(MASTER_ADDR)

    logging = Logger.get_default_logger()
    logging.info("Client: %s", client)

    client.create('a')
