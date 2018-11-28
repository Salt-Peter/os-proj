import ast

import master.chunk_manager as ch_mgr
import master.namespace_manager as ns_mgr
from commons.datastructures import ChunkInfo
from commons.loggers import default_logger

log = default_logger

SEPARATOR = "|||"


class OplogActions:
    ADD_CHUNK, GRANT_CLIENT_ID, CREATE_FILE, CREATE_DIR, DELETE_FILE, NOTIFY_MASTER, \
    REPORT_CHUNK, DEL_BAD_CHUNK = range(8)


def update_metadata(metadata_file, action, data):
    # TODO:
    #  - Maintain meta data as an in memory object
    #  - Find a way to dump entire meta data object
    with open(metadata_file, mode="a") as fp:
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

        elif key == OplogActions.NOTIFY_MASTER:
            log.debug("action NOTIFY_MASTER")
            m.chunk_manager.active_chunk_servers.add(value)

        elif key == OplogActions.CREATE_FILE:
            log.debug("action CREATE_FILE")

            path = value
            m.namespace_manager.paths[path] = ns_mgr.Path(False, 0)

        elif key == OplogActions.CREATE_DIR:
            log.debug("action CREATE_DIR")

            path = value
            m.namespace_manager.paths[path] = ns_mgr.Path(True, 0)

        elif key == OplogActions.DELETE_FILE:
            log.debug("action DELETE_FILE")

            path = value
            del m.namespace_manager.paths[path]

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

        # Chunkserver specific actions
        elif key == OplogActions.REPORT_CHUNK:
            path, chunk_handle, chunk_index, length = ast.literal_eval(value)
            m.chunks[chunk_handle] = ChunkInfo(path, chunk_handle, chunk_index, length)

        elif key == OplogActions.DEL_BAD_CHUNK:
            del m.chunks[value]

        else:
            log.error('Invalid meta data key: %s with value: %s', key, value)


def load_metadata(server):
    try:
        with open(server.metadata_file) as fp:
            parse_metadata(server, fp)
        log.debug("****oplog replay completed****")
    except FileNotFoundError:
        log.error("Can't open meta data file: %s", server.metadata_file)
