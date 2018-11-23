from commons.loggers import Logger

logging = Logger.get_default_logger()


def update_metadata(master):
    # TODO:
    #  - Maintain meta data as an in memory object
    #  - Find a way to dump entire meta data object
    with open(master.metadata_file, mode="w") as fp:
        fp.write(f"client_id {master.client_id}\n")


def parse_metadata(master, fp):
    # TODO: Load entire metadata object into memory
    key, value = fp.readline().split()
    if key == "client_id":
        master.client_id = int(value)
    else:
        logging.error('Invalid master meta data key: %s with value: %s', key, value)


def load_metadata(master):
    try:
        with open(master.metadata_file) as fp:
            parse_metadata(master, fp)
    except FileNotFoundError:
        logging.debug("Can't open meta data file: %s", master.metadata_file)
