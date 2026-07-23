#!/usr/bin/env python3
"""Replay saved SAM-style CDC envelopes through handler.lambda_handler (local DB).

Usage:
  python scripts/replay_samples.py --dir events/examples
  python scripts/replay_samples.py --dir data/sqs_live --max 20
"""
from __future__ import annotations

import argparse
import importlib
import json
import os
import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "lambda"))


def apply_env(fe: dict) -> None:
    os.environ["LOCAL_DEV"] = str(fe.get("LOCAL_DEV", "true")).lower()
    for k in (
        "DB_HOST",
        "DB_PORT",
        "DB_USER",
        "DB_PASSWORD",
        "DB_NAME",
        "RDS_SCHEMA_BOA_APP_RDS_DATA",
        "HANDLER_VERSION",
    ):
        if k in fe:
            os.environ[k] = str(fe[k])
    os.environ.setdefault("RDS_SCHEMA_BOA_APP_RDS_DATA", "boa_app_rds_direct")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--dir", default="events/examples", help="Directory of *.json envelopes"
    )
    ap.add_argument("--env-file", default="env.json")
    ap.add_argument("--max", type=int, default=0, help="Max files (0 = all)")
    args = ap.parse_args()

    env_path = ROOT / args.env_file
    if not env_path.exists():
        env_path = ROOT / "env.json.example"
    fe = (
        json.loads(env_path.read_text()).get("CDCHandler", {})
        if env_path.exists()
        else {}
    )
    apply_env(fe)

    handler = importlib.import_module("handler")
    importlib.reload(handler)

    class Ctx:
        aws_request_id = f"replay-{uuid.uuid4().hex[:12]}"

    files = sorted(Path(args.dir).glob("*.json"))
    if args.max:
        files = files[: args.max]
    ok = fail = 0
    for path in files:
        ev = json.loads(path.read_text())
        if "Records" not in ev:
            print(f"SKIP {path.name}: not an envelope")
            continue
        resp = handler.lambda_handler(ev, Ctx())
        failures = resp.get("batchItemFailures") or []
        if failures:
            fail += 1
            print(f"FAIL {path.name} {failures}")
        else:
            ok += 1
            print(f"OK   {path.name}")
    print(f"Done: ok={ok} fail={fail}")
    return 1 if fail else 0


if __name__ == "__main__":
    raise SystemExit(main())
