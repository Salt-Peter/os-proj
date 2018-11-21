import threading
from xmlrpc.server import SimpleXMLRPCServer

from numpy import uint64

from commons.loggers import Logger
from commons.settings import MASTER_PORT, MASTER_HOST, MASTER_ADDR


class Master:
    __slots__ = 'client_id_counter', 'chunk_handle', 'mutex', 'meta_file'

    def __init__(self):
        self.client_id_counter = 0
        self.chunk_handle = uint64(0)
        self.mutex = threading.Lock()  # TODO: probably use a re entrant lock
        self.meta_file = f'master_meta{MASTER_ADDR}'

    def test_ok(self):
        """A quick test to see if server is working fine"""
        return "Ok"

    def unique_client_id(self):
        """
        When a client is attached to the master,
        it calls this function to get a unique client ID.
        """
        logging.info("UNIQUE_CLIENT_ID API called")

        with self.mutex:
            self.client_id_counter += 1
            return self.client_id_counter
            # todo: Update master's meta data file

    def create(self, path):
        """Will be called by client to create a file in the namespace"""
        logging.info("CREATE API called")
        # TODO: implement
        return False, "Not Implemented yet"


if __name__ == '__main__':
    logging = Logger.get_request_logger()

    master_server = SimpleXMLRPCServer((MASTER_HOST, MASTER_PORT),
                                       logRequests=True,
                                       allow_none=True)

    # Read: https://gist.github.com/abnvanand/199cacf6c8f45258ff096b842b77b216
    master_server.register_introspection_functions()

    # register all methods to be available to client
    # can either use register_function(<function's_name>)
    # or register_instance(<class's_instance>)  # All the methods of the instance are published as XML-RPC methods
    master_server.register_instance(Master())

    master_server.serve_forever()
