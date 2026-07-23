"""Map CDC SQS event rows to PostgreSQL payloads.

Expected envelope shape (record body JSON):

    {
      "table": "notes" | "note_topics",
      "operation": "create" | "update" | "delete" | "upsert",
      "row": { ... source columns ... }
    }

Notes use composite primary key id = "{sid}-{boa_id}" in the destination DB.
Topic events only carry note_id (boa_id); sid is resolved via note lookup.
"""

from __future__ import annotations

from typing import Any


def parse_author_name(author_name: str | None) -> tuple[str | None, str | None]:
    """Split 'Last, First M.' into (first, last) for advisor name columns."""
    if not author_name:
        return None, None

    # Strip suffix after comma (e.g. credentials) before tokenizing.
    name_part = author_name.split(",")[0].strip()
    if not name_part:
        return None, None

    parts = name_part.split()
    if len(parts) == 0:
        return None, None
    if len(parts) == 1:
        return parts[0], None
    return " ".join(parts[:-1]), parts[-1]


def is_delete_operation(operation: str, row: dict[str, Any]) -> bool:
    """True when operation is delete or row carries deleted_at (soft delete)."""
    deleted_at = row.get("deleted_at")
    return operation.lower() == "delete" or (deleted_at is not None)


def effective_operation(operation: str, row: dict[str, Any]) -> str:
    """Normalize CDC operation for direct-path audit log (advising_notes_cdc_log)."""
    if is_delete_operation(operation, row):
        return "delete"
    if operation.lower() in ("create", "update", "upsert"):
        return "upsert"
    return operation.lower()


def map_note_row_to_payload(row: dict[str, Any]) -> dict[str, Any]:
    """Map source note row to advising_notes / advising_notes_delta column dict."""
    first_name, last_name = parse_author_name(row.get("author_name"))

    note_id = str(row["id"]) if row.get("id") is not None else None
    sid = row.get("sid")
    if not sid or not note_id:
        raise ValueError("Note row missing required 'sid' or 'id' field for composite id")
    composite_id = f"{sid}-{note_id}"

    return {
        "id": composite_id,
        "sid": sid,
        "boa_id": note_id,
        "advisor_uid": row.get("author_uid"),
        "author_name": row.get("author_name"),
        "advisor_first_name": first_name,
        "advisor_last_name": last_name,
        "subject": row.get("subject"),
        "note_body": row.get("body"),
        "is_private": row.get("is_private", False),
        "created_at": row.get("created_at"),
        "updated_at": row.get("updated_at"),
    }


def map_topic_row_to_payload(row: dict[str, Any]) -> dict[str, Any]:
    """Map topic row; composite id and sid are filled after note lookup in delta/direct."""
    note_id = str(row["note_id"]) if row.get("note_id") is not None else None
    return {
        "id": None,
        "sid": None,
        "boa_id": note_id,
        "topic": row.get("topic"),
    }
