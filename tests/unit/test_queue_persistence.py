import pytest
from unittest.mock import AsyncMock, MagicMock, patch


@pytest.mark.asyncio
async def test_failed_email_not_marked_as_read():
    """If _run_ai_pipeline fails, email must NOT be marked as read."""
    mock_ctx = MagicMock()
    mock_ctx.db_manager = AsyncMock()
    mock_ctx.db_manager.log_initial_email = AsyncMock(return_value=True)
    mock_ctx.db_manager.update_status = AsyncMock()
    mock_ctx.email_processor = MagicMock()
    mock_ctx.email_processor.process_email = MagicMock(return_value=True)
    mock_ctx.exchange_client = AsyncMock()
    mock_ctx.exchange_client.mark_as_read = AsyncMock()

    email_data = {
        "id": "fail-test",
        "subject": "Test",
        "body": "Hello",
        "sender": "a@b.com",
        "received_at": "2026-01-01",
    }

    with patch("src.exchange_service._upload_attachments_to_lark", new_callable=AsyncMock), \
         patch("src.exchange_service._ingest_to_qdrant", new_callable=AsyncMock), \
         patch("src.exchange_service._run_ai_pipeline", new_callable=AsyncMock, side_effect=Exception("LLM down")), \
         patch("src.exchange_service._mark_email_read", new_callable=AsyncMock) as mock_mark:

        from src.exchange_service import process_and_archive_email
        await process_and_archive_email(email_data, mock_ctx)

        mock_mark.assert_not_called()


@pytest.mark.asyncio
async def test_successful_email_marked_as_read():
    """On success, email should be marked as read."""
    mock_ctx = MagicMock()
    mock_ctx.db_manager = AsyncMock()
    mock_ctx.db_manager.log_initial_email = AsyncMock(return_value=True)
    mock_ctx.db_manager.update_status = AsyncMock()

    email_data = {
        "id": "success-test",
        "subject": "Test",
        "body": "Hello",
        "sender": "a@b.com",
        "received_at": "2026-01-01",
    }

    pipeline_result = {
        "classification": {"need_reply": False},
        "draft": "",
        "context": [],
        "email": email_data,
    }

    with patch("src.exchange_service._upload_attachments_to_lark", new_callable=AsyncMock), \
         patch("src.exchange_service._ingest_to_qdrant", new_callable=AsyncMock), \
         patch("src.exchange_service._run_ai_pipeline", new_callable=AsyncMock, return_value=pipeline_result), \
         patch(
             "src.exchange_service._dispatch_notification",
             new_callable=AsyncMock,
             return_value={"delivered": True, "kind": "skipped"},
         ), \
         patch("src.exchange_service._mark_email_read", new_callable=AsyncMock) as mock_mark:

        from src.exchange_service import process_and_archive_email
        await process_and_archive_email(email_data, mock_ctx)

        mock_mark.assert_called_once()
