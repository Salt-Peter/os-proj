import threading
from typing import Dict


class Path:
    is_dir: bool
    length: int

    __slots__ = 'is_dir', 'length'

    def __init__(self, is_dir=False, length=0):
        self.is_dir = is_dir
        self.length = length


class NamespaceManager:
    paths: Dict[str, Path]

    __slots__ = 'mutex', 'paths'

    def __init__(self):
        self.mutex = threading.Lock()  # TODO: probably use a re entrant lock
        # Initialize paths with root path
        self.paths = {'/': Path(True, 0)}  # TODO: probably need to persist in metadata file of master server

    # Create a file
    def create(self, path: str):
        with self.mutex:
            parent = get_parent(path)
            if not self.exists(parent):
                return False, "Path does not exist."

            if not self.is_dir(parent):
                return False, "Parent is not a directory."

            if self.exists(path):
                return False, "File already exists"

            self.paths[path] = Path(False, 0)

            return True, None

    def exists(self, path: str):
        return path in self.paths

    def is_dir(self, path: str):
        if path in self.paths and self.paths[path].is_dir:
            return True
        else:
            return False


def get_parent(path):
    idx = path.rfind('/')
    if idx in (-1, 0):  # if path='' or '/'
        return '/'

    # FIXME: what if path is = '/home/username/' i.e. ends with a trailing '/'
    #   I don't know whether it's a valid case
    return path[:idx]
