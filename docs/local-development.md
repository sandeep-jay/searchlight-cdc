# Local development

Setup, testing, and local validation for the direct-table CDC handler in this pack.

## Prerequisites

- Python 3.11+
- PostgreSQL (local or Docker)
- AWS SAM CLI (optional, for `sam local invoke`)
- Terraform >= 1.5 (for deploy validation only)
- AWS credentials (only for live SQS consume)

## First-time setup

```bash
cp env.json.example env.json   # never commit env.json
pip install -r requirements.txt
make apply-sql                 # empty DB only — see migrations.md
```

### env.json

Copy from `env.json.example`. The local SAM key is **`CDCHandler`**:

```json
{
  "CDCHandler": {
    "LOCAL_DEV": "true",
    "DB_HOST": "localhost",
    "DB_PORT": "5432",
    "DB_USER": "test_user",
    "DB_PASSWORD": "...",
    "DB_NAME": "test_db",
    "AWS_REGION": "us-west-2",
    "SQS_QUEUE_NAME": "your-queue.fifo",
    "RDS_SCHEMA_BOA_APP_RDS_DATA": "boa_app_rds_direct",
    "HANDLER_VERSION": "direct-v1"
  },
  "CDCHandlerProduction": {
    "LOCAL_DEV": "false",
    "DB_SECRET_NAME": "your-db-secret-name",
    "DB_NAME": "your_db",
    "RDS_SCHEMA_BOA_APP_RDS_DATA": "boa_app_rds_direct",
    "HANDLER_VERSION": "direct-v1"
  }
}
```

`CDCHandlerProduction` mirrors production env vars for reference; SAM local uses `CDCHandler`.

## Tests and lint

```bash
make test-unit    # Mocked unit tests (no Postgres required)
make check        # Ruff lint + format check
```

This pack includes **unit tests only** (`test/unit/`). Integration tests against a real Postgres instance live in the parent Searchlight repo.

## SAM local invoke (synthetic events)

No real data required — uses committed `events/examples/`:

```bash
make sam-test       # notes-create only
make sam-test-all   # all six event types
```

Equivalent manual invoke:

```bash
sam build
LOCAL_DEV=true sam local invoke CDCHandler \
  --env-vars env.json \
  --event events/examples/notes-create.json
```

Event catalog: [events/examples/README.md](../events/examples/README.md).

## Replay samples (no SAM)

Replay JSON envelopes directly through the handler:

```bash
make replay
# or
python scripts/replay_samples.py --dir events/examples
```

Uses credentials from `env.json` (`CDCHandler` block).

## Apply SQL locally

```bash
make apply-sql
# or
python scripts/apply_sql.py
```

Runs `sql/001_schema.sql`, `002_tables.sql`, `003_indexes.sql` in order with placeholder substitution.

**Warning:** `002_tables.sql` drops existing tables. Safe only on an empty local database. See [Migrations](migrations.md).

## Live SQS consume (QA queue)

The consume script **never deletes messages** from the queue. It is for QA validation only.

```bash
make consume-sqs-dry-run    # verify queue access, no DB writes
make consume-sqs            # receive + invoke handler
```

Manual options:

```bash
python scripts/sqs_consume.py --env-key CDCHandler --dry-run --max-messages 5
python scripts/sqs_consume.py --env-key CDCHandler --max-messages 10
```

### Pre-flight checklist (live queue)

- [ ] Target schema is **`boa_app_rds_direct`**
- [ ] Production Lambda event source **disabled** or visibility timeout long enough to avoid double consume
- [ ] `env.json` points at the intended queue (`SQS_QUEUE_NAME`)
- [ ] You understand messages will **not** be deleted (non-destructive QA only)

## Verify CDC log after replay

```sql
SELECT received_at, event_id, table_name, effective_operation,
       composite_id, apply_status
FROM boa_app_rds_direct.advising_notes_cdc_log
ORDER BY received_at DESC
LIMIT 20;
```

## Local data directory

If you capture live queue samples for debugging, store them outside this pack (e.g. a gitignored `data/` folder). **Never commit** real student or advisor data.

## Makefile reference

| Target | Description |
|--------|-------------|
| `make help` | List all targets |
| `make build` | `sam build` |
| `make apply-sql` | Apply `sql/*.sql` |
| `make test-unit` | `pytest test/unit/` |
| `make sam-test` | SAM invoke `notes-create.json` |
| `make sam-test-all` | SAM invoke all 6 examples |
| `make replay` | Replay `events/examples/` |
| `make consume-sqs` | Live SQS → handler |
| `make consume-sqs-dry-run` | Queue access check only |
| `make check` | Ruff lint + format |
