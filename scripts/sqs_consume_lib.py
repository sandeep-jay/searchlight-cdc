"""Shared non-destructive SQS consumer for local QA testing.

Used by sqs_consume_delta.py and sqs_consume_direct.py. Long-polls a queue,
optionally invokes handler.lambda_handler per message,
and never calls DeleteMessage (messages reappear after visibility timeout).

Default: non-destructive (messages stay on queue). Supports --dry-run, --save-dir,
Messages are never deleted from the queue.
"""
from __future__ import annotations

import argparse
import importlib
import json
import logging
import os
import sys
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import boto3

ROOT = Path(__file__).resolve().parents[1]
log = logging.getLogger("sqs_consume")

QUEUE_ATTRS = (
    "ApproximateNumberOfMessages",
    "ApproximateNumberOfMessagesNotVisible",
    "ApproximateNumberOfMessagesDelayed",
)


@dataclass
class HandlerConfig:
    """Per-handler wiring for sqs_consume.py."""

    handler_module: str  # Python module name under lambda/ (e.g. 'handler')
    handler_label: str  # Short name for logs
    default_env_key: str  # env.json section (CDCHandler or CDCHandlerDirect)
    default_schema: str  # Expected RDS schema for log output
    apply_handler_env: Callable[[dict], None]  # Set os.environ before handler import
    doc: str  # Module docstring used as argparse description


def setup_logging(level: str, log_file: str | None) -> None:
    log.setLevel(getattr(logging, level.upper(), logging.INFO))
    fmt = logging.Formatter(
        "%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    log.handlers.clear()
    log.addHandler(sh)
    log.propagate = False
    if log_file:
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setFormatter(fmt)
        log.addHandler(fh)


def load_env_file(path: str, key: str) -> dict:
    try:
        data = json.loads(Path(path).read_text())
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(data, dict):
        return {}
    section = data.get(key, data)
    return section if isinstance(section, dict) else {}


def _real(*vals: Any) -> Any:
    for v in vals:
        if v and not str(v).strip().lower().startswith(
            ("your-", "<", "changeme", "replace")
        ):
            return v
    return None


def resolve_queue(sqs: Any, queue: str) -> str:
    if queue.startswith("http://") or queue.startswith("https://"):
        return queue
    return sqs.get_queue_url(QueueName=queue)["QueueUrl"]


def queue_depth(sqs: Any, queue_url: str) -> dict[str, int]:
    attrs = sqs.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=list(QUEUE_ATTRS)
    )
    raw = attrs.get("Attributes", {})
    return {
        "visible": int(raw.get("ApproximateNumberOfMessages", 0)),
        "in_flight": int(raw.get("ApproximateNumberOfMessagesNotVisible", 0)),
        "delayed": int(raw.get("ApproximateNumberOfMessagesDelayed", 0)),
    }


def format_depth(d: dict[str, int]) -> str:
    total = d["visible"] + d["in_flight"] + d["delayed"]
    return (
        f"visible={d['visible']:,} in_flight={d['in_flight']:,} "
        f"delayed={d['delayed']:,} approx_total={total:,}"
    )


def to_envelope(msg: dict, queue_arn: str, region: str) -> dict:
    return {
        "Records": [
            {
                "messageId": msg.get("MessageId"),
                "receiptHandle": msg.get("ReceiptHandle", "consume-no-delete"),
                "body": msg.get("Body", ""),
                "attributes": msg.get("Attributes", {}),
                "messageAttributes": msg.get("MessageAttributes", {}),
                "md5OfBody": msg.get("MD5OfBody", ""),
                "eventSource": "aws:sqs",
                "eventSourceARN": queue_arn,
                "awsRegion": region,
            }
        ]
    }


def message_label(body: dict) -> str:
    table = body.get("table", "?")
    op = body.get("operation", "?")
    row = body.get("row") or {}
    note_id = row.get("id") or row.get("note_id") or "?"
    return f"{table}:{op}:id={note_id}"


def log_progress(
    processed: int,
    ok: int,
    failed: int,
    skipped_dup: int,
    start: float,
    depth: dict[str, int],
    *,
    prefix: str = "Progress",
) -> None:
    elapsed = time.time() - start
    rate = processed / elapsed if elapsed > 0 else 0.0
    log.info(
        "%s: processed=%s ok=%s failed=%s skipped_dup=%s elapsed=%.1fs rate=%.1f/s queue[%s]",
        prefix,
        processed,
        ok,
        failed,
        skipped_dup,
        elapsed,
        rate,
        format_depth(depth),
    )


def run_consume(handler_cfg: HandlerConfig, argv: list[str] | None = None) -> int:
    """CLI entry: poll SQS, invoke handler, report summary. Non-destructive by default."""
    ap = argparse.ArgumentParser(
        description=handler_cfg.doc,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument(
        "--queue", default=None, help="Queue name or URL (else env / env.json)"
    )
    ap.add_argument(
        "--env-file", default="env.json", help="JSON config (default: env.json)"
    )
    ap.add_argument(
        "--env-key",
        default=handler_cfg.default_env_key,
        help=f"Section in env file (default: {handler_cfg.default_env_key})",
    )
    ap.add_argument("--region", default=None)
    ap.add_argument("--profile", default=None)
    ap.add_argument(
        "--max-messages", type=int, default=10, help="Stop after N messages processed"
    )
    ap.add_argument("--max-seconds", type=int, default=300, help="Wall-clock limit")
    ap.add_argument(
        "--visibility",
        type=int,
        default=120,
        help="VisibilityTimeout on receive (seconds)",
    )
    ap.add_argument(
        "--wait", type=int, default=20, help="Long-poll WaitTimeSeconds (max 20)"
    )
    ap.add_argument(
        "--idle-polls", type=int, default=3, help="Stop after N empty polls"
    )
    ap.add_argument(
        "--dry-run", action="store_true", help="Receive only; do not invoke handler"
    )
    ap.add_argument(
        "--save-dir", default=None, help="Write each envelope as <messageId>.json"
    )
    ap.add_argument(
        "--progress-every",
        type=int,
        default=100,
        help="Log progress summary every N processed messages (0 to disable)",
    )
    ap.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        help="Logging level (DEBUG logs every message)",
    )
    ap.add_argument("--log-file", default=None, help="Also append logs to this file")
    args = ap.parse_args(argv)

    setup_logging(args.log_level, args.log_file)

    fe = load_env_file(str(ROOT / args.env_file), args.env_key)
    queue = _real(
        args.queue,
        os.environ.get("SQS_QUEUE_URL"),
        os.environ.get("SQS_QUEUE_NAME"),
        fe.get("SQS_QUEUE_URL"),
        fe.get("SQS_QUEUE_NAME"),
    )
    region = _real(args.region, os.environ.get("AWS_REGION"), fe.get("AWS_REGION"))
    profile = _real(args.profile, os.environ.get("AWS_PROFILE"), fe.get("AWS_PROFILE"))

    if not queue:
        log.error(
            "No queue configured. Use --queue or set SQS_QUEUE_NAME in env / env.json."
        )
        return 2

    if not args.dry_run:
        handler_cfg.apply_handler_env(fe)
        sys.path.insert(0, str(ROOT / "lambda"))
        handler = importlib.import_module(handler_cfg.handler_module)
        importlib.reload(handler)
    else:
        handler = None

    session = boto3.Session(profile_name=profile, region_name=region)
    region = session.region_name or region or "us-west-2"
    sqs = session.client("sqs")

    try:
        queue_url = resolve_queue(sqs, queue)
    except Exception as exc:  # noqa: BLE001
        log.error("Could not resolve queue %r: %s", queue, exc)
        return 2

    attrs = sqs.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=["QueueArn", "FifoQueue"]
    )
    queue_arn = attrs["Attributes"].get(
        "QueueArn", f"arn:aws:sqs:{region}:000000000000:{queue}"
    )
    is_fifo = attrs["Attributes"].get("FifoQueue") == "true"

    save_dir = Path(args.save_dir) if args.save_dir else None
    if save_dir:
        save_dir.mkdir(parents=True, exist_ok=True)

    schema = fe.get("RDS_SCHEMA_BOA_APP_RDS_DATA", handler_cfg.default_schema)
    start_depth = queue_depth(sqs, queue_url)

    log.info("Queue:     %s", queue_url)
    log.info("Region:    %s  fifo=%s", region, is_fifo)
    log.info(
        "Handler:   %s",
        "DRY-RUN (no invoke)" if args.dry_run else handler_cfg.handler_label,
    )
    log.info("DB schema: %s", schema)
    log.info("Mode:      NON-DESTRUCTIVE — messages are NOT deleted from queue")
    log.info(
        "Limits:    max_messages=%s max_seconds=%s visibility=%ss idle_polls=%s wait=%ss",
        args.max_messages,
        args.max_seconds,
        args.visibility,
        args.idle_polls,
        args.wait,
    )
    log.info("Queue depth at start: %s", format_depth(start_depth))

    class Ctx:
        aws_request_id = f"consume-{uuid.uuid4().hex[:12]}"

    processed = 0
    ok = 0
    failed = 0
    skipped_dup = 0
    seen_ids: set[str] = set()
    start = time.time()
    empty_streak = 0
    stop_reason = "unknown"
    results: list[tuple[str, bool, str]] = []
    last_progress_at = 0

    while processed < args.max_messages:
        if time.time() - start > args.max_seconds:
            stop_reason = f"max-seconds reached ({args.max_seconds}s)"
            log.warning("Stopping: %s", stop_reason)
            break
        if empty_streak >= args.idle_polls:
            depth = queue_depth(sqs, queue_url)
            stop_reason = (
                f"{empty_streak} consecutive empty polls "
                f"(idle_polls={args.idle_polls}); queue may be empty or all messages in-flight "
                f"(visibility={args.visibility}s). Depth: {format_depth(depth)}"
            )
            log.warning("Stopping: %s", stop_reason)
            break

        resp = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=min(10, args.max_messages - processed),
            WaitTimeSeconds=min(args.wait, 20),
            VisibilityTimeout=args.visibility,
            AttributeNames=["All"],
            MessageAttributeNames=["All"],
        )
        msgs = resp.get("Messages", [])
        if not msgs:
            empty_streak += 1
            depth = queue_depth(sqs, queue_url)
            log.info(
                "Empty poll %s/%s (wait=%ss). Queue depth: %s",
                empty_streak,
                args.idle_polls,
                min(args.wait, 20),
                format_depth(depth),
            )
            continue
        empty_streak = 0
        log.debug("Received batch of %s message(s)", len(msgs))

        for msg in msgs:
            if processed >= args.max_messages:
                break
            mid = msg.get("MessageId") or f"unknown-{processed}"
            if mid in seen_ids:
                skipped_dup += 1
                log.debug("Skipping duplicate messageId in this run: %s", mid)
                continue
            seen_ids.add(mid)

            try:
                body = json.loads(msg.get("Body", "{}"))
            except json.JSONDecodeError as exc:
                failed += 1
                results.append((mid, False, f"invalid JSON: {exc}"))
                log.error("[FAIL] %s invalid JSON: %s", mid, exc)
                processed += 1
                continue

            label = message_label(body)
            envelope = to_envelope(msg, queue_arn, region)

            if save_dir:
                out_path = save_dir / f"{mid}.json"
                out_path.write_text(json.dumps(envelope, indent=2, default=str) + "\n")

            if args.dry_run:
                ok += 1
                results.append((mid, True, f"dry-run {label}"))
                log.debug("[dry-run] %s  %s", mid, label)
                processed += 1
            else:
                Ctx.aws_request_id = f"consume-{uuid.uuid4().hex[:12]}"
                try:
                    resp_handler = handler.lambda_handler(envelope, Ctx())
                    failures = resp_handler.get("batchItemFailures") or []
                    if failures:
                        failed += 1
                        results.append(
                            (mid, False, f"{label} batchItemFailures={failures}")
                        )
                        log.error("[FAIL] %s  %s  %s", mid, label, failures)
                    else:
                        ok += 1
                        results.append((mid, True, label))
                        log.debug("[OK]   %s  %s", mid, label)
                except Exception as exc:  # noqa: BLE001
                    failed += 1
                    results.append((mid, False, f"{label} error={exc}"))
                    log.error("[FAIL] %s  %s  %s", mid, label, exc)
                processed += 1

            if (
                args.progress_every
                and processed - last_progress_at >= args.progress_every
            ):
                log_progress(
                    processed,
                    ok,
                    failed,
                    skipped_dup,
                    start,
                    queue_depth(sqs, queue_url),
                )
                last_progress_at = processed
    else:
        stop_reason = f"max-messages reached ({args.max_messages})"

    end_depth = queue_depth(sqs, queue_url)
    elapsed = time.time() - start
    rate = processed / elapsed if elapsed > 0 else 0.0

    log.info("=== Summary ===")
    log.info("  stop_reason:  %s", stop_reason)
    log.info("  processed:    %s", processed)
    log.info("  ok:           %s", ok)
    log.info("  failed:       %s", failed)
    log.info("  skipped_dup:  %s", skipped_dup)
    log.info("  unique_ids:   %s", len(seen_ids))
    log.info("  elapsed:      %.1fs (%.1f msg/s)", elapsed, rate)
    log.info("  queue start:  %s", format_depth(start_depth))
    log.info("  queue end:    %s", format_depth(end_depth))
    if save_dir:
        log.info("  saved:        %s/", save_dir)
    log.info(
        "  note: messages return to queue after visibility timeout (%ss)",
        args.visibility,
    )

    if failed:
        log.error("Failures (%s):", failed)
        for mid, success, detail in results:
            if not success:
                log.error("  %s: %s", mid, detail)
        return 1
    return 0
