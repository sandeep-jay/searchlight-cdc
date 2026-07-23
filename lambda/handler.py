"""Direct-table CDC Lambda (handler.lambda_handler).

SQS FIFO → lambda_handler → batch.run_sqs_batch → process_message

Write model (~2–3 SQL touches per event) plus advising_notes_cdc_log audit row.
Orphan topics → advising_note_topics_pending until note arrives.

Environment: RDS_SCHEMA_BOA_APP_RDS_DATA (default boa_app_rds_direct), HANDLER_VERSION.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from typing import Any

import psycopg2
import psycopg2.extras

from batch import run_sqs_batch
from logging_utils import log
from mapping import effective_operation, is_delete_operation, map_note_row_to_payload

SERVICE_NAME = "advising-notes-search-cdc-direct"

# ---------------------------------------------------------------------------
# Table configuration (env-driven; re-read on each load_tables() call)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DirectTables:
    """Fully-qualified PostgreSQL table names for the direct schema."""

    schema: str
    notes: str
    topics: str
    fts: str
    pending_topics: str
    cdc_log: str
    handler_version: str


def load_tables() -> DirectTables:
    """Return table names from environment with boa_app_rds_direct defaults."""
    schema = os.environ.get("RDS_SCHEMA_BOA_APP_RDS_DATA", "boa_app_rds_direct")
    return DirectTables(
        schema=schema,
        notes=os.environ.get("NOTES_TABLE", f"{schema}.advising_notes"),
        topics=os.environ.get("TOPICS_TABLE", f"{schema}.advising_note_topics"),
        fts=os.environ.get("FTS_TABLE", f"{schema}.advising_notes_search_index"),
        pending_topics=os.environ.get(
            "PENDING_TOPICS_TABLE", f"{schema}.advising_note_topics_pending"
        ),
        cdc_log=os.environ.get("CDC_LOG_TABLE", f"{schema}.advising_notes_cdc_log"),
        handler_version=os.environ.get("HANDLER_VERSION", "direct-v1"),
    )


# ---------------------------------------------------------------------------
# CDC writer (single-table model + audit log)
# ---------------------------------------------------------------------------


def process_message(
    conn: psycopg2.extensions.connection, msg: dict[str, Any], message_id: str
) -> None:
    """Route one CDC envelope; audit log written in the same transaction."""
    table = msg.get("table", "")
    operation = msg.get("operation", "").lower()
    row = msg.get("row", {})

    log(
        SERVICE_NAME,
        logging.INFO,
        "Processing message",
        event_id=message_id,
        table=table,
        operation=operation,
    )

    with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:  # noqa: SIM117
        if table == "notes":
            process_note(cur, operation, row, message_id)
        elif table == "note_topics":
            process_note_topic(cur, operation, row, message_id)
        else:
            raise ValueError(f"Unsupported table: {table}")


def insert_cdc_log(
    cur: psycopg2.extensions.cursor,
    t: Any,
    *,
    event_id: str,
    table_name: str,
    operation: str,
    row: dict[str, Any],
    prepared_record: dict[str, Any] | None,
    composite_id: str | None,
    boa_id: str | None,
    apply_status: str,
) -> None:
    """Append advising_notes_cdc_log row (applied | parked | partial_warning)."""
    cur.execute(
        f"""
        INSERT INTO {t.cdc_log} (
            event_id, table_name, operation, effective_operation,
            boa_id, composite_id, payload, prepared_record,
            apply_status, applied_at, handler_version
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, now(), %s)
        ON CONFLICT (event_id) DO UPDATE SET
            effective_operation = EXCLUDED.effective_operation,
            boa_id = EXCLUDED.boa_id,
            composite_id = EXCLUDED.composite_id,
            payload = EXCLUDED.payload,
            prepared_record = EXCLUDED.prepared_record,
            apply_status = EXCLUDED.apply_status,
            applied_at = EXCLUDED.applied_at,
            handler_version = EXCLUDED.handler_version
        """,
        (
            event_id,
            table_name,
            operation,
            effective_operation(operation, row),
            boa_id,
            composite_id,
            json.dumps(row),
            json.dumps(prepared_record) if prepared_record is not None else None,
            apply_status,
            t.handler_version,
        ),
    )


def process_note(
    cur: psycopg2.extensions.cursor,
    operation: str,
    row: dict[str, Any],
    event_id: str,
) -> None:
    """Upsert or delete a note; writes CDC audit row in the same transaction."""
    t = load_tables()

    if is_delete_operation(operation, row):
        note_id = str(row.get("id")) if row.get("id") is not None else None
        sid = row.get("sid")
        if not note_id or not sid:
            raise ValueError("Note row missing required 'id' or 'sid' field for composite id")

        composite_id = f"{sid}-{note_id}"
        cur.execute(f"DELETE FROM {t.notes} WHERE id = %s", (composite_id,))
        cur.execute(f"DELETE FROM {t.topics} WHERE id = %s", (composite_id,))
        cur.execute(f"DELETE FROM {t.fts} WHERE id = %s", (composite_id,))
        cur.execute(f"DELETE FROM {t.pending_topics} WHERE boa_id = %s", (note_id,))
        insert_cdc_log(
            cur,
            t,
            event_id=event_id,
            table_name="notes",
            operation=operation,
            row=row,
            prepared_record={"composite_id": composite_id},
            composite_id=composite_id,
            boa_id=note_id,
            apply_status="applied",
        )
        log(
            SERVICE_NAME,
            logging.INFO,
            "Note deleted",
            event_id=event_id,
            composite_id=composite_id,
            boa_id=note_id,
        )
        return

    if operation in ("create", "update", "upsert"):
        payload = map_note_row_to_payload(row)
        note_id = payload.get("id")
        if not note_id:
            raise ValueError("Note row missing required 'id' field")

        sql = f"""
            INSERT INTO {t.notes} (
                id, sid, boa_id, advisor_uid, author_name,
                advisor_first_name, advisor_last_name,
                subject, note_body, is_private, created_at, updated_at
            )
            VALUES (
                %(id)s, %(sid)s, %(boa_id)s, %(advisor_uid)s, %(author_name)s,
                %(advisor_first_name)s, %(advisor_last_name)s,
                %(subject)s, %(note_body)s, %(is_private)s, %(created_at)s, %(updated_at)s
            )
            ON CONFLICT (id)
            DO UPDATE SET
                sid = EXCLUDED.sid,
                boa_id = EXCLUDED.boa_id,
                advisor_uid = EXCLUDED.advisor_uid,
                author_name = EXCLUDED.author_name,
                advisor_first_name = EXCLUDED.advisor_first_name,
                advisor_last_name = EXCLUDED.advisor_last_name,
                subject = EXCLUDED.subject,
                note_body = EXCLUDED.note_body,
                is_private = EXCLUDED.is_private,
                created_at = EXCLUDED.created_at,
                updated_at = EXCLUDED.updated_at
        """
        cur.execute(sql, payload)
        reconcile_pending_topics(
            cur, t, note_id, payload.get("sid"), payload.get("boa_id"), event_id
        )
        fts_status = update_fts_index(cur, t, note_id, event_id)
        insert_cdc_log(
            cur,
            t,
            event_id=event_id,
            table_name="notes",
            operation=operation,
            row=row,
            prepared_record=payload,
            composite_id=note_id,
            boa_id=payload.get("boa_id"),
            apply_status="partial_warning" if fts_status == "warning" else "applied",
        )
        log(
            SERVICE_NAME,
            logging.INFO,
            "Note upserted",
            event_id=event_id,
            composite_id=note_id,
            boa_id=payload.get("boa_id"),
        )
        return

    raise ValueError(f"Unsupported operation: {operation}")


def process_note_topic(
    cur: psycopg2.extensions.cursor,
    operation: str,
    row: dict[str, Any],
    event_id: str,
) -> None:
    """Upsert/delete topic; park in pending if parent note not yet seen."""
    t = load_tables()
    boa_id = str(row.get("note_id")) if row.get("note_id") is not None else None
    topic = row.get("topic")

    if not boa_id:
        raise ValueError("Topic row missing required 'note_id' field")
    if not topic:
        raise ValueError("Topic row missing required 'topic' field")

    cur.execute(f"SELECT id, sid FROM {t.notes} WHERE boa_id = %s", (boa_id,))
    note_row = cur.fetchone()

    if not note_row:
        if is_delete_operation(operation, row):
            cur.execute(
                f"DELETE FROM {t.pending_topics} WHERE boa_id = %s AND topic = %s",
                (boa_id, topic),
            )
            insert_cdc_log(
                cur,
                t,
                event_id=event_id,
                table_name="note_topics",
                operation=operation,
                row=row,
                prepared_record=None,
                composite_id=None,
                boa_id=boa_id,
                apply_status="applied",
            )
            log(
                SERVICE_NAME,
                logging.INFO,
                "Pending topic removed (note not present)",
                event_id=event_id,
                boa_id=boa_id,
                topic=topic,
            )
        else:
            cur.execute(
                f"""
                INSERT INTO {t.pending_topics} (boa_id, topic, author_uid)
                VALUES (%s, %s, %s)
                ON CONFLICT (boa_id, topic) DO UPDATE SET author_uid = EXCLUDED.author_uid
                """,
                (boa_id, topic, row.get("author_uid")),
            )
            insert_cdc_log(
                cur,
                t,
                event_id=event_id,
                table_name="note_topics",
                operation=operation,
                row=row,
                prepared_record={"boa_id": boa_id, "topic": topic},
                composite_id=None,
                boa_id=boa_id,
                apply_status="parked",
            )
            log(
                SERVICE_NAME,
                logging.INFO,
                "Topic parked pending note arrival",
                event_id=event_id,
                boa_id=boa_id,
                topic=topic,
            )
        return

    composite_id = note_row.get("id") if hasattr(note_row, "get") else note_row[0]
    sid = note_row.get("sid") if hasattr(note_row, "get") else note_row[1]
    prepared = {"id": composite_id, "sid": sid, "boa_id": boa_id, "topic": topic}

    if is_delete_operation(operation, row):
        cur.execute(
            f"DELETE FROM {t.topics} WHERE id = %s AND topic = %s",
            (composite_id, topic),
        )
        fts_status = update_fts_index(cur, t, composite_id, event_id)
        insert_cdc_log(
            cur,
            t,
            event_id=event_id,
            table_name="note_topics",
            operation=operation,
            row=row,
            prepared_record=prepared,
            composite_id=composite_id,
            boa_id=boa_id,
            apply_status="partial_warning" if fts_status == "warning" else "applied",
        )
        log(
            SERVICE_NAME,
            logging.INFO,
            "Topic deleted",
            event_id=event_id,
            composite_id=composite_id,
            boa_id=boa_id,
            topic=topic,
        )
        return

    if operation in ("create", "update", "upsert"):
        cur.execute(
            f"""
            INSERT INTO {t.topics} (id, sid, boa_id, topic)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (id, topic)
            DO UPDATE SET sid = EXCLUDED.sid, boa_id = EXCLUDED.boa_id
            """,
            (composite_id, sid, boa_id, topic),
        )
        fts_status = update_fts_index(cur, t, composite_id, event_id)
        insert_cdc_log(
            cur,
            t,
            event_id=event_id,
            table_name="note_topics",
            operation=operation,
            row=row,
            prepared_record=prepared,
            composite_id=composite_id,
            boa_id=boa_id,
            apply_status="partial_warning" if fts_status == "warning" else "applied",
        )
        log(
            SERVICE_NAME,
            logging.INFO,
            "Topic upserted",
            event_id=event_id,
            composite_id=composite_id,
            boa_id=boa_id,
            topic=topic,
        )
        return

    raise ValueError(f"Unsupported operation: {operation}")


def reconcile_pending_topics(
    cur: psycopg2.extensions.cursor,
    t: Any,
    composite_id: str,
    sid: str,
    boa_id: str,
    event_id: str,
) -> int:
    """Attach topics that arrived before this note."""
    cur.execute(
        f"""
        INSERT INTO {t.topics} (id, sid, boa_id, topic)
        SELECT %s, %s, boa_id, topic FROM {t.pending_topics} WHERE boa_id = %s
        ON CONFLICT (id, topic) DO NOTHING
        """,
        (composite_id, sid, boa_id),
    )
    moved = cur.rowcount if isinstance(cur.rowcount, int) else 0
    cur.execute(f"DELETE FROM {t.pending_topics} WHERE boa_id = %s", (boa_id,))
    if moved:
        log(
            SERVICE_NAME,
            logging.INFO,
            "Reconciled pending topics",
            event_id=event_id,
            composite_id=composite_id,
            boa_id=boa_id,
            count=moved,
        )
    return moved


def update_fts_index(
    cur: psycopg2.extensions.cursor,
    t: Any,
    composite_id: str,
    event_id: str,
) -> str:
    """Rebuild FTS for one note from notes + topics tables. Returns applied or warning."""
    sql = f"""
        WITH note_data AS (
            SELECT id, subject, note_body, author_name
            FROM {t.notes}
            WHERE id = %s
        ),
        topic_agg AS (
            SELECT id, STRING_AGG(topic, ' ' ORDER BY topic) AS topics_text
            FROM {t.topics}
            WHERE id = %s
            GROUP BY id
        ),
        fts_content AS (
            SELECT
                nd.id,
                to_tsvector(
                    'english',
                    COALESCE(nd.subject, '')     || ' ' ||
                    COALESCE(nd.note_body, '')   || ' ' ||
                    COALESCE(ta.topics_text, '') || ' ' ||
                    COALESCE(nd.author_name, '')
                ) AS fts_index
            FROM note_data nd
            LEFT JOIN topic_agg ta ON nd.id = ta.id
        )
        INSERT INTO {t.fts} (id, fts_index)
        SELECT id, fts_index FROM fts_content
        WHERE id IS NOT NULL
        ON CONFLICT (id) DO UPDATE SET fts_index = EXCLUDED.fts_index
    """
    cur.execute(sql, (composite_id, composite_id))
    if cur.rowcount == 0:
        log(
            SERVICE_NAME,
            logging.WARNING,
            "FTS index update resulted in no rows affected",
            event_id=event_id,
            composite_id=composite_id,
        )
        return "warning"
    log(
        SERVICE_NAME,
        logging.INFO,
        "FTS index updated",
        event_id=event_id,
        composite_id=composite_id,
    )
    return "applied"


# ---------------------------------------------------------------------------
# Lambda entry
# ---------------------------------------------------------------------------


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """AWS Lambda entry: process SQS batch into direct tables with CDC audit log."""
    return run_sqs_batch(event, context, process_message, SERVICE_NAME)


__all__ = [
    "lambda_handler",
    "process_message",
    "load_tables",
    "DirectTables",
]
