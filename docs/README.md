# Direct production pack — documentation

Self-contained docs for the **direct-table CDC** Lambda in this folder. BOA remains the source of truth; this system maintains a derived search index in PostgreSQL schema `boa_app_rds_direct`.

## Guides

| Document | When to use |
|----------|-------------|
| [Architecture](architecture.md) | How the handler works: schema, events, FTS, CDC audit log |
| [Local development](local-development.md) | Setup, tests, SAM, replay, live SQS consume |
| [Deployment](deployment.md) | Terraform, secrets, VPC, post-deploy verification |
| [Operations](operations.md) | Logs, DLQ, integrity checks, full refresh |
| [Migrations](migrations.md) | SQL safety — **read before touching RDS** |

## Quick commands

```bash
cp env.json.example env.json
pip install -r requirements.txt

make apply-sql              # empty local DB only — see migrations.md
make test-unit              # mocked unit tests
make sam-test               # SAM invoke notes-create
make sam-test-all           # all 6 synthetic events
make replay                 # replay events/examples through handler
make consume-sqs-dry-run    # verify queue access (no DB writes)
make consume-sqs            # live QA queue — never deletes messages
```

Deploy:

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars   # edit values
terraform init && terraform apply
```

See [Deployment](deployment.md) for secret format and required variables.

## Project layout

```text
direct-prod/
├── lambda/handler.py       # handler.lambda_handler
├── sql/                    # DDL 001–003 for boa_app_rds_direct
├── terraform/              # Lambda + IAM + SQS event source
├── events/examples/        # Synthetic CDC envelopes
├── scripts/                # apply_sql, replay, sqs_consume
├── test/unit/              # Unit tests (mocked DB)
└── docs/                   # This folder
```
