from commons.loggers import Logger
from commons.settings import MASTER_ADDR
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


if __name__ == "__main__":
    client = get_instance(MASTER_ADDR)

    logging = Logger.get_default_logger()
    logging.info("Client: %s", client)

    create('a', client)
