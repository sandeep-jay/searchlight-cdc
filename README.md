# searchlight-cdc

Direct-table **CDC Lambda** for advising notes: SQS FIFO → Lambda → PostgreSQL.

This repo is bootstrapped in small, reviewable PRs. This first PR adds only repo scaffolding.

## Requirements

- Python 3.11

## Local setup

```bash
python3.11 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## What comes next

Follow-up PRs will add, in order:

1. PostgreSQL schema and migrations (`sql/`)
2. CDC Lambda handler, unit tests, and sample events (`lambda/`, `test/`, `events/`)

Local dev tooling, Terraform, and CI/CD will land in later PRs.

## Security

Do **not** commit local secrets or credentials. The root `.gitignore` blocks common leak paths (`env.json`, `terraform.tfvars`, `.venv/`, keys, etc.). Use example/template files with placeholders only.
