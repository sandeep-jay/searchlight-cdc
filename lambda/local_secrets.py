"""Local DB credentials for SAM local invoke and pytest when LOCAL_DEV=true."""

import os


def get_local_db_credentials() -> dict:
    """Return PostgreSQL connection dict from DB_* environment variables."""
    return {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": int(os.getenv("DB_PORT", 5432)),
        "username": os.getenv("DB_USER", "test_user"),
        "password": os.getenv("DB_PASSWORD", "test_password"),
    }
