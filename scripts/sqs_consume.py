#!/usr/bin/env python3
"""
Non-destructive SQS consumer for local QA testing.

Polls the queue, invokes handler.lambda_handler per message.
Messages are never deleted from the queue.

Usage:
  python scripts/sqs_consume.py
  python scripts/sqs_consume.py --dry-run --max-messages 5
  python scripts/sqs_consume.py --max-messages 100 --save-dir data/sqs_live
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from sqs_consume_lib import HandlerConfig, run_consume  # noqa: E402


def apply_handler_env(fe: dict) -> None:
    os.environ["LOCAL_DEV"] = str(fe.get("LOCAL_DEV", "true")).lower()
    for key in (
        "DB_HOST",
        "DB_PORT",
        "DB_USER",
        "DB_PASSWORD",
        "DB_NAME",
        "RDS_SCHEMA_BOA_APP_RDS_DATA",
        "HANDLER_VERSION",
        "NOTES_TABLE",
        "TOPICS_TABLE",
        "FTS_TABLE",
        "CDC_LOG_TABLE",
        "PENDING_TOPICS_TABLE",
    ):
        if key in fe and fe[key] is not None:
            os.environ[key] = str(fe[key])
    for key in (
        "NOTES_DELTA_TABLE",
        "NOTES_NIGHTLY_TABLE",
        "TOPICS_DELTA_TABLE",
        "TOPICS_NIGHTLY_TABLE",
        "FTS_DELTA_TABLE",
        "FTS_NIGHTLY_TABLE",
    ):
        os.environ.pop(key, None)
    os.environ.setdefault("RDS_SCHEMA_BOA_APP_RDS_DATA", "boa_app_rds_direct")
    os.environ.setdefault("HANDLER_VERSION", "direct-v1")


CONFIG = HandlerConfig(
    handler_module="handler",
    handler_label="direct",
    default_env_key="CDCHandler",
    default_schema="boa_app_rds_direct",
    apply_handler_env=apply_handler_env,
    doc=__doc__,
)

if __name__ == "__main__":
    raise SystemExit(run_consume(CONFIG))
