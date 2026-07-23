# Operations

Day-to-day monitoring, incident response, and recovery for the direct-table CDC handler deployed from this pack.

## Log group

| Resource | CloudWatch path |
|----------|-----------------|
| Direct handler | `/aws/lambda/{project_name}-{environment}-cdc-handler` |

Default example: `/aws/lambda/advising-notes-search-direct-qa-cdc-handler`

Logs are **structured JSON**:

```json
{
  "level": "INFO",
  "service": "advising-notes-search-cdc-direct",
  "message": "Message processed successfully",
  "event_id": "<sqs messageId>"
}
```

Log retention: 30 days (set in `terraform/lambda.tf`).

## Alarms

This standalone Terraform stack creates a CloudWatch log group but **does not** provision alarms or dashboards. If you need operational alarms, add them separately or use org-wide monitoring. Recommended thresholds:

| Alarm | Trigger |
|-------|---------|
| Lambda errors | >= 5 errors in 5 minutes |
| DLQ depth | >= 1 message |
| Queue backlog | >= 1000 messages (15 min avg) |
| Lambda duration | Avg >= 80% of timeout |
| Lambda throttles | Any throttle |

## DLQ

Failed messages land in the DLQ after `maxReceiveCount` receives on the main FIFO queue.

**Response:**

1. Inspect CloudWatch logs for `event_id`
2. Fix root cause (schema, data, code)
3. Redrive/replay from DLQ (idempotent — see [Idempotent replay](#idempotent-replay))

## Direct-path audit log

Query recent applies:

```sql
SELECT received_at, event_id, table_name, effective_operation,
       composite_id, apply_status
FROM boa_app_rds_direct.advising_notes_cdc_log
ORDER BY received_at DESC
LIMIT 50;
```

| `apply_status` | Meaning |
|----------------|---------|
| `applied` | Committed successfully |
| `parked` | Orphan topic waiting for note |
| `partial_warning` | Committed but FTS updated 0 rows |

## When to pause ingest

- Sustained Lambda error rate
- DLQ depth > 0 unresolved
- Integrity query failures (see [Integrity checks](#integrity-checks))

Disable the SQS event source mapping in AWS Console or via Terraform until the issue is cleared.

---

## Idempotent replay

All CDC operations are keyed by composite `id` (notes) or `(id, topic)` (topics). Replaying SQS messages or audit log entries is safe.

### DLQ replay

1. Fix handler or data issue
2. Move messages from DLQ back to main queue (AWS console / script)
3. Monitor CloudWatch for success

## Integrity checks

Run after incidents or before declaring healthy.

**Notes missing FTS:**

```sql
SELECT n.id
FROM boa_app_rds_direct.advising_notes n
LEFT JOIN boa_app_rds_direct.advising_notes_search_index s ON n.id = s.id
WHERE s.id IS NULL;
```

**Orphan pending topics (may be normal briefly):**

```sql
SELECT count(*) FROM boa_app_rds_direct.advising_note_topics_pending;
```

**Duplicate note ids (should be 0):**

```sql
SELECT id, count(*)
FROM boa_app_rds_direct.advising_notes
GROUP BY id
HAVING count(*) > 1;
```

## Failure signals

| Signal | Likely cause | Action |
|--------|--------------|--------|
| DLQ messages | Handler exceptions | Fix + replay DLQ |
| `partial_warning` in CDC log | Note missing during FTS rebuild | Investigate ordering; replay note event |
| Duplicate note ids | Data corruption | Pause CDC; investigate + full refresh |
| Sustained `parked` topics | Note events missing | Check upstream CDC; replay note events |

## Full refresh

When a nightly export reloads base tables:

1. **Pause** ingest (disable SQS event source mapping)
2. **Reload** base tables from authoritative export (external ETL — do not use `002_tables.sql` on RDS; see [Migrations](migrations.md))
3. **Replay** CDC from audit log for events after the export cutoff:

```sql
SELECT event_id, payload, table_name, operation, received_at
FROM boa_app_rds_direct.advising_notes_cdc_log
WHERE apply_status IN ('applied', 'partial_warning')
  AND received_at > :nightly_cutoff
ORDER BY received_at;
```

Convert log rows to SQS envelopes and replay via `scripts/replay_samples.py` or a custom script.

4. **Run integrity checks**
5. **Re-enable** event source mapping

## Point-in-time single note

If one note is wrong:

1. Find latest log entry for `boa_id` in `advising_notes_cdc_log`
2. Replay single envelope via SAM (`make sam-test` with edited event) or `scripts/replay_samples.py`
3. Or re-emit from BOA source if available
