import logging


def get_logger(name='blackfynn-python'):
    """
    Returns a logger configured to be used throughout the
    blackfynn-python library

    Args:
      name (string): Name of the logger, default "blackfynn-python"

    Returns:
      Logger
    """
    logger = logging.getLogger(name)

    logger.setLevel(logging.INFO)
    return logger
