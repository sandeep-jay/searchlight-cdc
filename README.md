# searchlight-cdc

**Direct-table CDC Lambda** for advising notes — consumes change events from an existing **SQS FIFO** queue and writes to **PostgreSQL** (`boa_app_rds_direct`).

```
SQS FIFO (CDC events) → handler.lambda_handler → PostgreSQL
```

Each message updates notes, topics, full-text search, and an audit log in a single transaction. Failed messages are reported back to SQS for retry (`ReportBatchItemFailures`).

## Features

- Python 3.11 Lambda handler with batch processing and structured logging
- PostgreSQL schema and ordered SQL migrations
- Local development via SAM, pytest, and helper scripts
- Non-destructive SQS consumer for QA testing (never deletes queue messages)
- Attaches to an **existing** SQS queue — does not provision queues

## Quick start

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cp env.json.example env.json   # edit with your local DB settings
make apply-sql                 # empty local DB only — see docs/migrations.md
make test-unit
make sam-test                  # invoke handler with a sample CDC event
```

## Documentation

| Topic | Link |
|-------|------|
| Overview | [docs/README.md](docs/README.md) |
| Architecture | [docs/architecture.md](docs/architecture.md) |
| Local development | [docs/local-development.md](docs/local-development.md) |
| Migrations | [docs/migrations.md](docs/migrations.md) |
| Operations | [docs/operations.md](docs/operations.md) |

## Project layout

| Path | Purpose |
|------|---------|
| `lambda/` | CDC handler and runtime modules |
| `sql/` | PostgreSQL DDL (`001`–`003`) |
| `test/` | Unit tests |
| `events/examples/` | Synthetic SQS CDC payloads for local invoke |
| `scripts/` | SQL apply, event replay, SQS consume |
| `template.yaml` | SAM template for local testing |

## Deploy

Terraform stack and deployment guide are planned for a follow-up PR. The handler expects:

- An existing SQS FIFO queue (event source mapping)
- AWS Secrets Manager secret with DB credentials
- RDS PostgreSQL with `boa_app_rds_direct` schema migrated

## Security

Do **not** commit `env.json`, `terraform.tfvars`, or other credentials. Use `env.json.example` and `terraform.tfvars.example` with placeholders only.
