"""Structured JSON logging for CDC Lambda handlers.

Each log line is one JSON object emitted to stdout (CloudWatch in AWS):

    {"level": "INFO", "service": "advising-notes-search-cdc", "message": "...",
     "event_id": "...", "boa_id": "...", ...}

Set LOG_LEVEL env (DEBUG, INFO, WARNING, ERROR) to control verbosity.
"""

from __future__ import annotations

import json
import logging
import os
import traceback
from typing import Any

_LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logger = logging.getLogger("cdc")
logger.setLevel(getattr(logging, _LOG_LEVEL, logging.INFO))
if not logger.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(_handler)
    logger.propagate = False


def log(service_name: str, level: int, msg: str, **kwargs: Any) -> None:
    """Emit a structured JSON log line with optional context fields."""
    payload: dict[str, Any] = {
        "level": logging.getLevelName(level),
        "service": service_name,
        "message": msg,
    }
    payload.update(kwargs)
    logger.log(level, json.dumps(payload, default=str))


def log_exception(
    service_name: str,
    msg: str,
    exc: BaseException,
    **kwargs: Any,
) -> None:
    """Log an error with exception type, message, and stack trace."""
    log(
        service_name,
        logging.ERROR,
        msg,
        error_type=type(exc).__name__,
        error=str(exc),
        traceback=traceback.format_exc(),
        **kwargs,
    )
