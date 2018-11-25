import ast

import master.chunk_manager as ch_mgr
import master.namespace_manager as ns_mgr
from commons.loggers import default_logger
from commons.settings import OP_LOG_FILENAME

log = default_logger

SEPARATOR = "|||"


class OplogActions:
    ADD_CHUNK, GRANT_CLIENT_ID, CREATE_FILE = range(3)


def update_metadata(action, data):
    # TODO:
    #  - Maintain meta data as an in memory object
    #  - Find a way to dump entire meta data object
    with open(OP_LOG_FILENAME, mode="a") as fp:
        fp.write(f"{action}{SEPARATOR}{data}\n")


def parse_metadata(m, fp):
    # TODO: simplify it's too complicated and hard coded
    log.debug("****Beginning oplog replay****")

    for line in fp:
        line = line.strip()
        key, value = line.split(SEPARATOR)
        key = int(key)

        if key == OplogActions.GRANT_CLIENT_ID:
            log.debug("action GRANT_CLIENT_ID")
            m.client_id = int(value)

        elif key == OplogActions.CREATE_FILE:
            log.debug("action CREATE_FILE")

            path = value
            m.namespace_manager.paths[path] = ns_mgr.Path(False, 0)

        elif key == OplogActions.ADD_CHUNK:
            log.debug("Replaying add chunk")

            path, chunk_index, handle, locations, chunk_handle_counter = ast.literal_eval(value)

            # update our dicts
            chunk = m.chunk_manager.chunks.get(path, None)
            if not chunk:  # prevent NPE when updating chunks[path][chunk_index]
                m.chunk_manager.chunks[path] = {}

            m.chunk_manager.chunks[path][chunk_index] = ch_mgr.Chunk(handle)
            m.chunk_manager.locations[handle] = ch_mgr.ChunkInfo(handle, locations)
            m.chunk_manager.handles[handle] = ch_mgr.PathIndex(path, chunk_index)
            m.chunk_manager.chunk_handle = chunk_handle_counter

        else:
            log.error('Invalid master meta data key: %s with value: %s', key, value)


def load_metadata(master):
    try:
        with open(master.metadata_file) as fp:
            parse_metadata(master, fp)
        log.debug("****oplog replay completed****")
    except FileNotFoundError:
        log.error("Can't open meta data file: %s", master.metadata_file)
