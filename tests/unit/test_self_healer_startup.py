import pytest
from datetime import date
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from src.safety.input_limits import InputLimitExceeded


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


def _self_healing_context(email: dict) -> MagicMock:
    cursor = MagicMock()
    cursor.execute = AsyncMock()
    cursor.__aenter__ = AsyncMock(return_value=cursor)
    cursor.__aexit__ = AsyncMock(return_value=False)

    connection = MagicMock()
    connection.cursor.return_value = cursor
    connection.__aenter__ = AsyncMock(return_value=connection)
    connection.__aexit__ = AsyncMock(return_value=False)

    ctx = MagicMock()
    ctx.exchange_client = MagicMock()
    ctx.exchange_client.get_email = AsyncMock(return_value=email)
    ctx.db_manager = MagicMock()
    ctx.db_manager.get_connection.return_value = connection
    ctx.db_manager.log_initial_email = AsyncMock()
    return ctx


@pytest.mark.asyncio
async def test_reprocess_single_forces_retry_without_predelete():
    from src.utils import self_healing

    email = {"id": "exchange-id", "body": "ok", "attachments": []}
    ctx = _self_healing_context(email)

    with patch(
        "src.utils.self_healing.process_and_archive_email",
        new_callable=AsyncMock,
    ) as mock_process:
        result = await self_healing.SelfHealer(ctx, 60).reprocess_single("mail-1")

    assert result is True
    ctx.db_manager.get_connection.assert_not_called()
    mock_process.assert_awaited_once_with(email, ctx, force_reprocess=True)


@pytest.mark.asyncio
async def test_reprocess_oversized_email_preserves_existing_record(caplog):
    from src.utils import self_healing

    private_body = "private-oversized-body"
    email = {"body": private_body, "attachments": []}
    ctx = _self_healing_context(email)
    real_process = self_healing.process_and_archive_email
    caplog.set_level(logging.ERROR, logger="SelfHealing")

    with patch(
        "src.utils.self_healing.process_and_archive_email",
        new=AsyncMock(wraps=real_process),
    ) as process_spy, patch(
        "src.exchange_service.get_settings",
        return_value=SimpleNamespace(EMAIL_BODY_MAX_BYTES=5),
    ):
        result = await self_healing.SelfHealer(ctx, 60).reprocess_single("mail-2")

    assert result is False
    ctx.db_manager.get_connection.assert_not_called()
    ctx.db_manager.log_initial_email.assert_not_awaited()
    process_spy.assert_awaited_once_with(email, ctx, force_reprocess=True)
    assert private_body not in caplog.text


@pytest.mark.asyncio
async def test_reprocess_input_limit_does_not_log_exception_text(caplog):
    from src.utils import self_healing

    private_body = "private-body-from-exception"

    class LeakyInputLimitExceeded(InputLimitExceeded):
        def __str__(self) -> str:
            return private_body

    email = {"body": "ok", "attachments": []}
    ctx = _self_healing_context(email)
    error = LeakyInputLimitExceeded("body_bytes")
    caplog.set_level(logging.ERROR, logger="SelfHealing")

    with patch(
        "src.utils.self_healing.process_and_archive_email",
        new_callable=AsyncMock,
        side_effect=error,
    ) as mock_process:
        result = await self_healing.SelfHealer(ctx, 60).reprocess_single("mail-3")

    assert result is False
    ctx.db_manager.get_connection.assert_not_called()
    mock_process.assert_awaited_once_with(email, ctx, force_reprocess=True)
    assert private_body not in caplog.text
