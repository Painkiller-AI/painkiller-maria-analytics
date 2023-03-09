import logging
import os
import sys

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")


def get_logger(name: str):
    logger = logging.getLogger(name)
    logger.setLevel(LOG_LEVEL)

    handler = logging.StreamHandler(stream=sys.stdout)
    logger.addHandler(handler)

    return logger
