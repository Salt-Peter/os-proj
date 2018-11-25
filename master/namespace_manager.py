import threading
from typing import Dict

from commons.errors import *


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
                return False, PathNotFoundErr

            if not self.is_dir(parent):
                return False, ParentIsNotDirErr

            if self.exists(path):
                return False, FileAlreadyExistsErr

            self.paths[path] = Path(False, 0)

            return True, None

    # Create a file
    def create_dir(self, path: str):
        with self.mutex:
            parent = get_parent(path)
            if not self.exists(parent):
                return False, PathNotFoundErr

            if not self.is_dir(parent):
                return False, ParentIsNotDirErr

            if self.exists(path):
                return False, DirAlreadyExistsErr

            self.paths[path] = Path(True, 0)

            return True, None

    # list all files
    def list(self, path: str):
        files = []
        with self.mutex:
            if not self.is_dir(path):
                return files, ParentIsNotDirErr

            if not self.exists(path):
                return files, PathNotFoundErr

            for file in self.paths:
                parent = get_parent(file)
                if parent == path:
                    files.append(file)

            return files, None

    # delete File
    def delete(self, path: str):
        if not self.exists(path):
            return PathNotFoundErr

        if self.is_dir(path):
            resp, err = self.list(path)
            if resp and len(resp) > 0:
                return DirIsNotEmptyErr

        del self.paths[path]
        return None

    def exists(self, path: str):
        return path in self.paths

    def is_dir(self, path: str):
        if path in self.paths and self.paths[path].is_dir:
            return True
        else:
            return False

    def get_file_length(self, path):
        with self.mutex:
            info = self.paths.get(path, None)
            if not info:
                return 0, FileNotFoundErr
            return info.length, None

    def set_file_length(self, path, length):
        with self.mutex:
            info = self.paths.get(path, None)
            if info:  # add check to prevent NPE
                info.length = length


def get_parent(path):
    import os
    return os.path.dirname(path)
