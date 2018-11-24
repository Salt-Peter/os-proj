# Define custom Loggers
import logging

default_formatter = logging.Formatter('[%(filename)s %(funcName)s] - [%(levelname)s] - %(message)s')
request_formatter = logging.Formatter('%(asctime)s - [%(filename)s %(funcName)s RPC] - [%(levelname)s] - %(message)s')
thread_formatter = logging.Formatter('%(asctime)s - [%(thread)d %(threadName)s] - [%(levelname)s] - %(message)s')


def setup_logger(name, formatter, level=logging.DEBUG):
    """Function setup as many loggers as you want"""

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger


default_logger = setup_logger('default_logger', default_formatter)
request_logger = setup_logger('request_logger', request_formatter)
