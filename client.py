from commons.loggers import Logger
from commons.settings import MASTER_ADDR, CHUNK_SIZE
from commons.utils import rpc_call


# data structure for client
class Client:
    # Read: https://docs.python.org/3/reference/datamodel.html#slots
    __slots__ = 'client_id', 'master_addr'


    def __repr__(self):
        return f'<client_id: {self.client_id}, master_addr: {self.master_addr}>'


def get_instance(master_addr):
    c = Client()
    c.master_addr = master_addr

    master_server = rpc_call(c.master_addr)
    # call master to get a unique client id
    c.client_id = master_server.unique_client_id()

    return c


# create a file
def create(path, c):
    master_server = rpc_call(c.master_addr)
    resp, err = master_server.create(path)
    if resp:
        logging.debug("Create API response %s", resp)
    else:
        logging.error("Error creating file '%s'. Why? : %s", path, err)


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
    client = get_instance(MASTER_ADDR)

    logging = Logger.get_default_logger()
    logging.info("Client: %s", client)

    create('a', client)
