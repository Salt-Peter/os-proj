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
        # TODO: use logging.debug, error etc instead of print
        print("File created successfully")
    else:
        print("Error creating file", path, err)


if __name__ == "__main__":
    client = get_instance(MASTER_ADDR)

    # print('Server test:', server.test_ok())
    print("Client:", client)

    create('a', client)
