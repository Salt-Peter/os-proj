# Useful data structures
# TODO: Probably switch from class to namedtuples


class ChunkInfo:
    path: str
    chunk_handle: int
    chunk_index: int
    length: int
    __slots__ = 'path', 'chunk_handle', 'chunk_index', 'length'

    def __init__(self, path, chunk_handle, chunk_index, length=0):
        self.path = path
        self.chunk_handle = chunk_handle
        self.chunk_index = chunk_index
        self.length = length


class DataId:
    client_id: int
    timestamp: float

    # if we use __slots__ then __dict__ will not be available
    __slots__ = 'client_id', 'timestamp'

    def __init__(self, client_id=-1, timestamp=-1):
        self.client_id = client_id
        self.timestamp = timestamp

    def __repr__(self):
        return f'DataId(client_id={self.client_id}, timestamp={self.timestamp})'
