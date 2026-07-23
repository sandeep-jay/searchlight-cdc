"""Shared SQS batch processing loop for CDC handlers.

Both delta and direct delegate here. Responsibilities:

  - Open one DB connection per Lambda invocation (transaction per message).
  - Parse each SQS record body and call the writer's process_message().
  - Report partial batch failures so FIFO retries only failed messageIds.
  - Roll back the current message on error; continue processing siblings.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from typing import Any

import psycopg2

from db import get_db_connection
from logging_utils import log, log_exception


def run_sqs_batch(
    event: dict[str, Any],
    context: Any,
    process_message: Callable[[psycopg2.extensions.connection, dict[str, Any], str], None],
    service_name: str,
) -> dict[str, Any]:
    """Process SQS FIFO records with partial batch failure reporting."""
    records = event.get("Records", [])
    request_id = getattr(context, "aws_request_id", "unknown")
    log(
        service_name,
        logging.INFO,
        "Lambda invocation started",
        request_id=request_id,
        record_count=len(records),
    )

    batch_item_failures: list[dict[str, str]] = []
    conn = None
    succeeded = 0

    try:
        conn = get_db_connection()
        log(service_name, logging.DEBUG, "Database connection opened", request_id=request_id)

        for record in records:
            message_id = record.get("messageId", "unknown")
            try:
                body = json.loads(record["body"])
                table = body.get("table")
                operation = body.get("operation")
                process_message(conn, body, message_id)
                succeeded += 1
                log(
                    service_name,
                    logging.INFO,
                    "Message processed successfully",
                    request_id=request_id,
                    message_id=message_id,
                    table=table,
                    operation=operation,
                )
            except Exception as e:
                log_exception(
                    service_name,
                    "Error processing message",
                    e,
                    request_id=request_id,
                    message_id=message_id,
                )
                batch_item_failures.append({"itemIdentifier": message_id})
                if conn and not conn.closed:
                    conn.rollback()  # failed message only; prior commits in this batch already persisted

    except Exception as e:
        log_exception(
            service_name,
            "Fatal error in Lambda handler",
            e,
            request_id=request_id,
        )
        batch_item_failures = [{"itemIdentifier": r.get("messageId", "unknown")} for r in records]
    finally:
        if conn and not conn.closed:
            conn.close()
            log(service_name, logging.DEBUG, "Database connection closed", request_id=request_id)

    failed = len(batch_item_failures)
    log(
        service_name,
        logging.INFO,
        "Lambda invocation complete",
        request_id=request_id,
        record_count=len(records),
        succeeded=succeeded,
        failed=failed,
    )
    return {"batchItemFailures": batch_item_failures}
