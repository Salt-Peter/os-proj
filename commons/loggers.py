# Define custom Loggers
import logging


# TODO: simplify it's too complicated

class Logger:
    @staticmethod
    def get_request_logger():
        logger = logging.getLogger(LogTypes.REQUEST_LOGGER)
        logger.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        formatter = logging.Formatter(LogFormats.REQUEST_FORMAT)
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        return logger

    @staticmethod
    def get_default_logger():
        logger = logging.getLogger(LogTypes.DEFAULT_LOGGER)
        logger.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        formatter = logging.Formatter(LogFormats.DEFAULT_FORMAT)
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        return logger


class LogFormats:
    DEFAULT_FORMAT = '[%(filename)s %(funcName)s] - [%(levelname)s] - %(message)s'
    REQUEST_FORMAT = '%(asctime)s - [%(filename)s %(funcName)s] - [%(levelname)s] - %(message)s'
    THREAD_FORMAT = '%(asctime)s - [%(thread)d %(threadName)s] - [%(levelname)s] - %(message)s'


class LogTypes:
    REQUEST_LOGGER = "REQUEST_LOGGER"
    DEFAULT_LOGGER = "DEFAULT_LOGGER"
    THREAD_LOGGER = "DEFAULT_LOGGER"
