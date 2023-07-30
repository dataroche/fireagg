import logging


def setup_logging():
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)
