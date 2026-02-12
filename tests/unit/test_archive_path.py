from unittest.mock import AsyncMock, MagicMock, patch

import pytest


@pytest.mark.asyncio
async def test_skip_analysis_skips_attachment_upload():
    from src.exchange_service import process_and_archive_email

    mock_ctx = MagicMock()
    mock_ctx.db_manager = AsyncMock()
    mock_ctx.db_manager.log_initial_email = AsyncMock(return_value=True)
    mock_ctx.db_manager.update_status = AsyncMock()
    mock_ctx.email_processor = MagicMock()
    mock_ctx.email_processor.process_email = MagicMock()
    mock_ctx.exchange_client = AsyncMock()
    mock_ctx.exchange_client.mark_as_read = AsyncMock(return_value=True)

    email_data = {
        "id": "SENT_001",
        "subject": "Test sent email",
        "sender": "me@example.com",
        "body": "<p>Hello</p>",
        "attachments": [{"name": "file.pdf", "content": "base64data"}],
        "_event_type": "CreatedEvent",
    }

    with patch("src.exchange_service._upload_attachments_to_lark") as mock_upload, patch(
        "src.exchange_service._ingest_to_qdrant"
    ) as mock_ingest, patch("src.exchange_service._mark_email_read") as mock_read:
        await process_and_archive_email(email_data, mock_ctx, skip_analysis=True)

        mock_upload.assert_not_called()
        mock_ingest.assert_called_once()
        mock_read.assert_not_called()


@pytest.mark.asyncio
async def test_full_pipeline_uploads_attachments():
    from src.exchange_service import process_and_archive_email

    mock_ctx = MagicMock()
    mock_ctx.db_manager = AsyncMock()
    mock_ctx.db_manager.log_initial_email = AsyncMock(return_value=True)
    mock_ctx.db_manager.update_status = AsyncMock()
    mock_ctx.email_processor = MagicMock()
    mock_ctx.email_processor.process_email = MagicMock()
    mock_ctx.exchange_client = AsyncMock()
    mock_ctx.graph = AsyncMock()

    email_data = {
        "id": "INBOX_001",
        "subject": "Test incoming email",
        "sender": "someone@example.com",
        "body": "<p>Hello</p>",
        "attachments": [],
        "_event_type": "NewMailEvent",
    }

    with patch("src.exchange_service._upload_attachments_to_lark") as mock_upload, patch(
        "src.exchange_service._ingest_to_qdrant"
    ) as mock_ingest, patch(
        "src.exchange_service._run_ai_pipeline",
        return_value={"classification": {"need_reply": False}},
    ) as mock_ai, patch("src.exchange_service._dispatch_notification") as mock_notify, patch(
        "src.exchange_service._mark_email_read"
    ) as mock_read:
        await process_and_archive_email(email_data, mock_ctx, skip_analysis=False)

        mock_upload.assert_called_once()
        mock_ingest.assert_called_once()
        mock_ai.assert_called_once()
        mock_notify.assert_called_once()
        mock_read.assert_called_once()
