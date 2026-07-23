"""Unit tests for direct CDC handler."""

from unittest.mock import MagicMock, Mock, patch

import pytest

from handler import lambda_handler


@pytest.mark.unit
class TestHandler:
    def test_note_upsert_success(self, mock_context, mock_secrets_manager, note_upsert_event):
        with patch("db.psycopg2.connect") as mock_connect:
            mock_conn = MagicMock()
            mock_cur = MagicMock()
            mock_conn.cursor.return_value.__enter__.return_value = mock_cur
            mock_conn.__enter__ = Mock(return_value=mock_conn)
            mock_conn.__exit__ = Mock(return_value=False)
            mock_connect.return_value = mock_conn

            result = lambda_handler(note_upsert_event, mock_context)

            assert result["batchItemFailures"] == []
            assert mock_cur.execute.call_count > 0

    def test_note_delete_success(self, mock_context, mock_secrets_manager, note_delete_event):
        with patch("db.psycopg2.connect") as mock_connect:
            mock_conn = MagicMock()
            mock_cur = MagicMock()
            mock_conn.cursor.return_value.__enter__.return_value = mock_cur
            mock_conn.__enter__ = Mock(return_value=mock_conn)
            mock_conn.__exit__ = Mock(return_value=False)
            mock_connect.return_value = mock_conn

            result = lambda_handler(note_delete_event, mock_context)

            assert result["batchItemFailures"] == []
            assert mock_cur.execute.call_count >= 3
