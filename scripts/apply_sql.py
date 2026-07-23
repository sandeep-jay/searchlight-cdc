#!/usr/bin/env python3
"""Apply sql/*.sql with schema placeholder substitution."""
from __future__ import annotations

import argparse
import os
from pathlib import Path

import psycopg2

SQL_DIR = Path(__file__).resolve().parents[1] / "sql"
FILES = ("001_schema.sql", "002_tables.sql", "003_indexes.sql")


def main() -> int:
    ap = argparse.ArgumentParser(description="Apply direct schema SQL")
    ap.add_argument("--host", default=os.environ.get("DB_HOST", "localhost"))
    ap.add_argument("--port", type=int, default=int(os.environ.get("DB_PORT", "5432")))
    ap.add_argument("--user", default=os.environ.get("DB_USER", "test_user"))
    ap.add_argument("--password", default=os.environ.get("DB_PASSWORD", ""))
    ap.add_argument("--dbname", default=os.environ.get("DB_NAME", "test_db"))
    ap.add_argument(
        "--schema",
        default=os.environ.get("RDS_SCHEMA_BOA_APP_RDS_DATA", "boa_app_rds_direct"),
    )
    args = ap.parse_args()

    subs = {
        "{rds_schema_boa_app_rds_data}": args.schema,
        "{rds_app_boa_user}": args.user,
    }
    conn = psycopg2.connect(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        dbname=args.dbname,
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        for name in FILES:
            sql = (SQL_DIR / name).read_text()
            for k, v in subs.items():
                sql = sql.replace(k, v)
            print(f"Applying {name} ...")
            cur.execute(sql)
    conn.close()
    print(f"Schema {args.schema} ready.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
