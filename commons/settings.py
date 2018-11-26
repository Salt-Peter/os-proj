# Read: https://www.python.org/dev/peps/pep-0008/#constants
# CHUNK_SIZE = 64 * (1 << 20)  # 64 MB
CHUNK_SIZE = 4  # 4 Bytes # FIXME remove in production
APPEND_SIZE = 2 # for testing purpose, idealy it shoud be 1/4th of the chunk size
REPLICATION_FACTOR = 3

DEFAULT_IP = "127.0.0.1"
DEFAULT_MASTER_PORT = 9001
DEFAULT_MASTER_ADDR = f'http://{DEFAULT_IP}:{DEFAULT_MASTER_PORT}'

OP_LOG_FILENAME = "master_metadata.txt"
