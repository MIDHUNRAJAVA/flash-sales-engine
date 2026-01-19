"""

 * Copyright (c) ConverSight.ai - All Rights Reserved

 * This material is proprietary to ConverSight.ai.

 * The methods and techniques described herein are considered trade secrets and/or confidential.

 * Reproduction or distribution, in whole or in part, is forbidden except by express written permission of ConverSight.ai.

"""
from logging import Logger
from pylogrus import TextFormatter,JsonFormatter, PyLogrus
import logging
import os

# Default log level
log_level = "INFO"

logger = Logger(__name__)
if os.getenv("LOG_LEVEL") is not None:
    log_level = os.getenv("LOG_LEVEL")
level = logging.getLevelName(log_level)


logging.setLoggerClass(PyLogrus)
logger = logging.getLogger(__name__)  # type: PyLogrus

if not logger.handlers:

    # reduce log level
    # logging.getLogger("pika").setLevel(logging.WARNING)
    # or, disable propagation
    # logging.getLogger("pika").propagate = False

    logger.setLevel(level)

    enabled_fields = [
            'asctime',
            'levelname',
            'message',
            'name'
        ]

    # Use JSON Formatter for production
    formatter = JsonFormatter(datefmt='Z', enabled_fields=enabled_fields, sort_keys=True)
    formatter.override_level_names({'CRITICAL': 'FATAL', 'WARNING': 'WARN'})

    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(formatter)

    logger.addHandler(ch)
    logger.propagate = False


def get_logger():
    return logger


def get_info(msg, args=""):
    return str(msg) + str(args)
