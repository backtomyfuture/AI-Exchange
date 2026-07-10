import pytest
from datetime import date
from unittest.mock import AsyncMock, MagicMock


@pytest.mark.asyncio
async def test_self_healer_get_stuck_emails_uses_pool():
    """SelfHealer should use get_connection async context manager."""
    mock_cursor = MagicMock()
    mock_cursor.fetchall = AsyncMock(return_value=[])
    mock_cursor.__aenter__ = AsyncMock(return_value=mock_cursor)
    mock_cursor.__aexit__ = AsyncMock(return_value=False)

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=False)

    mock_db = MagicMock()
    mock_db.get_connection.return_value = mock_conn

    mock_ctx = MagicMock()
    mock_ctx.db_manager = mock_db

    from src.utils.self_healing import SelfHealer
    healer = SelfHealer(ctx=mock_ctx, interval_seconds=60)
    result = await healer.get_stuck_emails()
    assert result == []
    mock_db.get_connection.assert_called_once()


@pytest.mark.asyncio
async def test_db_get_records_by_date():
    """db_manager.get_records_by_date should query emails for a specific date."""
    from contextlib import asynccontextmanager

    mock_cursor = AsyncMock()
    mock_cursor.fetchall = AsyncMock(return_value=[
        {"id": "1", "subject": "Test", "status": "sent"}
    ])
    mock_cursor.__aenter__ = AsyncMock(return_value=mock_cursor)
    mock_cursor.__aexit__ = AsyncMock(return_value=False)

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    @asynccontextmanager
    async def fake_get_connection():
        yield mock_conn

    from src.utils.db_async import AsyncDatabaseManager
    db = AsyncDatabaseManager.__new__(AsyncDatabaseManager)
    db._pool = MagicMock()
    db.get_connection = fake_get_connection

    result = await db.get_records_by_date(date(2026, 2, 12))
    assert len(result) == 1
    assert result[0]["subject"] == "Test"
