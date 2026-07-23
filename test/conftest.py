"""Pytest fixtures for direct CDC handler."""

import json
import os
import sys
from unittest.mock import Mock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lambda"))

import pytest  # noqa: E402


@pytest.fixture(autouse=True)
def setup_env_vars():
    os.environ["LOCAL_DEV"] = "true"
    os.environ["DB_NAME"] = os.getenv("DB_NAME", "test_db")
    os.environ["DB_HOST"] = os.getenv("DB_HOST", "localhost")
    os.environ["DB_PORT"] = os.getenv("DB_PORT", "5432")
    os.environ["DB_USER"] = os.getenv("DB_USER", "test_user")
    os.environ["DB_PASSWORD"] = os.getenv("DB_PASSWORD", "test_password")
    os.environ["RDS_SCHEMA_BOA_APP_RDS_DATA"] = os.getenv(
        "RDS_SCHEMA_BOA_APP_RDS_DATA", "boa_app_rds_direct"
    )
    os.environ["HANDLER_VERSION"] = "direct-v1"
    for k in (
        "NOTES_DELTA_TABLE",
        "NOTES_NIGHTLY_TABLE",
        "TOPICS_DELTA_TABLE",
        "TOPICS_NIGHTLY_TABLE",
        "FTS_DELTA_TABLE",
        "FTS_NIGHTLY_TABLE",
    ):
        os.environ.pop(k, None)
    yield


@pytest.fixture
def mock_context():
    context = Mock()
    context.aws_request_id = "test-request-id"
    return context


@pytest.fixture
def mock_secrets_manager():
    creds = {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": int(os.getenv("DB_PORT", 5432)),
        "username": os.getenv("DB_USER", "test_user"),
        "password": os.getenv("DB_PASSWORD", "test_password"),
    }
    with patch("db.secrets_client") as mock_secrets:
        mock_secrets.get_secret_value.return_value = {"SecretString": json.dumps(creds)}
        yield mock_secrets


@pytest.fixture
def note_upsert_event():
    return {
        "Records": [
            {
                "messageId": "test-message-1",
                "body": json.dumps(
                    {
                        "table": "notes",
                        "operation": "create",
                        "row": {
                            "id": 12345,
                            "sid": "SID001",
                            "body": "Student discussed graduation plan",
                            "author_uid": "uid123",
                            "author_name": "Jane Advisor",
                            "subject": "Lorem ipsum meeting notes",
                            "created_at": "2024-01-01T00:00:00Z",
                            "updated_at": "2024-01-01T00:00:00Z",
                        },
                    }
                ),
            }
        ]
    }


@pytest.fixture
def note_delete_event():
    return {
        "Records": [
            {
                "messageId": "test-message-2",
                "body": json.dumps(
                    {
                        "table": "notes",
                        "operation": "delete",
                        "row": {
                            "id": 99999,
                            "sid": "SID001",
                            "deleted_at": "2024-01-02T00:00:00Z",
                        },
                    }
                ),
            }
        ]
    }
