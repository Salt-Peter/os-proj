import xmlrpc.client


# Helper functions
def rpc_call(server_addr):
    return xmlrpc.client.ServerProxy(server_addr,
                                     verbose=False,
                                     allow_none=True)
