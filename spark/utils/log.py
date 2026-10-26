"""Logging setup shared by the Spark jobs and utility modules."""

import logging
import os

FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"


def get_logger(name: str) -> logging.Logger:
    """Return a configured logger for ``name``.

    The level comes from the ``LOG_LEVEL`` environment variable and defaults to
    ``INFO``. Calling this more than once is safe: ``basicConfig`` only configures
    the root logger the first time.
    """
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper(), format=FORMAT)
    return logging.getLogger(name)
