import threading
from xmlrpc.server import SimpleXMLRPCServer

from commons.loggers import Logger
from commons.settings import MASTER_ADDR, MASTER_PORT, MASTER_HOST, CHUNK_SIZE
from master.chunk_manager import ChunkManager
from master.metadata_manager import load_metadata, update_metadata
from master.namespace_manager import NamespaceManager


class Master:
    __slots__ = 'my_addr', 'client_id', 'chunk_handle', 'mutex', \
                'metadata_file', 'namespace_manager', 'chunk_manager'

    def __init__(self):
        self.my_addr = MASTER_ADDR
        self.client_id = 0  # counter to give next client ID
        self.chunk_handle = 0  # counter to give next chunk handle ID
        self.mutex = threading.Lock()  # TODO: probably use a re entrant lock
        self.metadata_file = 'master_metadata.txt'  # File that contains masters metadata

        self.namespace_manager = NamespaceManager()
        self.chunk_manager = ChunkManager()

        # self.chunk_servers = []

    def test_ok(self):
        """A quick test to see if server is working fine"""
        return "Ok"

    def unique_client_id(self):
        """
        When a client is attached to the master,
        it calls this function to get a unique client ID.
        """
        req_logging.info("UNIQUE_CLIENT_ID API called")

        with self.mutex:
            self.client_id += 1

            # make client id persistent in metadata file
            update_metadata(self)

            return self.client_id

    def create(self, path):
        """Will be called by client to create a file in the namespace"""
        req_logging.info("CREATE API called")
        res, err = self.namespace_manager.create(path)
        return res, err

    def add_chunk(self, path, chunk_index):
        req_logging.info("ADD CHUNK API called")
        info, err = self.chunk_manager.add_chunk(path, chunk_index)
        if err:
            return None, None, err

        print("HERE", info, type(info))
        return info.chunk_handle, info.locations, None

    def find_locations(self, path, chunk_index):
        """Returns chunk handle and an array of chunk locations for a given file name and chunk index"""
        chunk_locations, chunk_handle, err = self.chunk_manager.find_locations(path, chunk_index)

        return chunk_locations, chunk_handle, err

    # // FindLeaseHolder replies to client RPC to get the primary chunkserver for a
    # // given chunkhandle. If there is no current lease holder, chunkManager will
    # // automatically select one of the replicas to be the primary, and grant lease
    # // to that primary.
    def find_lease_holder(self, chunk_handle):
        req_logging.info("chunk_handle: %s", chunk_handle)
        lease, err = self.chunk_manager.find_lease_holder(chunk_handle)
        if err:
            return None, None, err

        return lease.primary, lease.expiration, None

    # // Chunk server calls ReportChunk to tell the master
    # // they have a certain chunk and the number of defined bytes in
    # // the chunk.
    def report_chunk(self, server, chunk_handle, chunk_index, length, path):
        req_logging.debug("Report chunk called")
        path_index, err = self.chunk_manager.get_path_index_from_handle(chunk_handle)
        if err:
            return None, err
        self.chunk_manager.set_chunk_location(chunk_handle, server)

        #   // Update file information
        file_length, err = self.namespace_manager.get_file_length(path_index.path)
        calculated = CHUNK_SIZE * path_index.index + length
        logging.debug("Result: %s, index: %s, length: %s", calculated, path_index.index, length)
        if calculated > file_length:
            self.namespace_manager.set_file_length(path_index.path, calculated)
            logging.debug("New length", calculated)

        return None

    def create_dir(self, path):
        """Will be called by client to create a dir in the namespace"""
        req_logging.info("CREATE DIR API called")
        res, err = self.namespace_manager.create_dir(path)
        return res, err

    def list(self, path):
        """Will be called by client to list all files present in given directory path"""
        req_logging.info("LIST FILES API called")
        res, err = self.namespace_manager.list(path)
        return res, err

    def delete(self, path):
        """Will be called by client to delete a specific file"""
        req_logging.info("DELETE FILE API called")
        err = self.namespace_manager.delete(path)
        return err

    def get_file_length(self, path):
        """Will be called by client to get the file length"""
        req_logging.info("GET FILE LENGTH API Called")
        file_length, err = self.namespace_manager.get_file_length(path)
        return file_length, err


def start_master():
    m = Master()

    # restore previous launch's meta data
    load_metadata(m)

    master_server = SimpleXMLRPCServer((MASTER_HOST, MASTER_PORT),
                                       logRequests=True,
                                       allow_none=True)

    # Read: https://gist.github.com/abnvanand/199cacf6c8f45258ff096b842b77b216
    master_server.register_introspection_functions()

    # register all methods to be available to client
    # can either use register_function(<function's_name>)
    # or register_instance(<class's_instance>)  # All the methods of the instance are published as XML-RPC methods
    master_server.register_instance(m)

    master_server.serve_forever()

    # TODO: launch background tasks (eg. gc, heartbeat) in a separate thread


if __name__ == '__main__':
    req_logging = Logger.get_request_logger()
    logging = Logger.get_default_logger()

    start_master()
