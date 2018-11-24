# Read: https://www.python.org/dev/peps/pep-0008/#constants
CHUNK_SIZE = 64 * (1 << 20)  # 64 MB
REPLICATION_FACTOR = 3

MASTER_HOST = "127.0.0.1"
MASTER_PORT = 9001
MASTER_ADDR = f'http://{MASTER_HOST}:{MASTER_PORT}'
