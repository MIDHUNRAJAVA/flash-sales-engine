"""

 * Copyright (c) ConverSight.ai - All Rights Reserved

 * This material is proprietary to ConverSight.ai.

 * The methods and techniques described herein are considered trade secrets and/or confidential.

 * Reproduction or distribution, in whole or in part, is forbidden except by express written permission of ConverSight.ai.

"""
import logging
import os

import structlog
from opentelemetry import trace

SERVICE = "order-service"
VERSION = os.getenv("SERVICE_VERSION", "unknown")


def _add_service(_logger, _method, event_dict):
    event_dict["service"] = SERVICE
    event_dict["version"] = VERSION
    return event_dict


def _add_trace_context(_logger, _method, event_dict):
    ctx = trace.get_current_span().get_span_context()
    if ctx and ctx.is_valid:
        event_dict["trace_id"] = format(ctx.trace_id, "032x")
        event_dict["span_id"] = format(ctx.span_id, "016x")
    return event_dict


_configured = False


def _configure():
    global _configured
    if _configured:
        return
    level = logging.getLevelName(os.getenv("LOG_LEVEL", "INFO"))
    if not isinstance(level, int):
        level = logging.INFO
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            _add_service,
            _add_trace_context,
            structlog.processors.TimeStamper(fmt="iso", key="ts"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )
    _configured = True


_configure()


def get_logger():
    return structlog.get_logger()


def get_info(msg, args=""):
    return str(msg) + str(args)
