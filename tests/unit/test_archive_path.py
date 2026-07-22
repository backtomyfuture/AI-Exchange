from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.storage import ContentRef


def _content_ref():
    from src.exchange_service import get_settings

    return ContentRef(
        account_id=get_settings().EXCHANGE_ACCOUNT_ID,
        object_id="00000000-0000-4000-8000-000000000037",
        key_version="v1",
        sha256="3" * 64,
    )


def _wire_content_store(ctx):
    ctx.content_store = AsyncMock()
    ref = _content_ref()
    ctx.content_store.put_email.return_value = ref
    ctx.db_manager.set_content_ref = AsyncMock()
    ctx.db_manager.set_content_ref_if_absent = AsyncMock(return_value=True)
    ctx.db_manager.get_content_ref = AsyncMock(return_value=ref)
    values = {}
    state = SimpleNamespace(values=values, next=())

    async def update_state(_config, delta, **kwargs):
        values.update(delta)
        if kwargs.get("as_node") == "__start__":
            state.next = ("categorizer",)

    ctx.graph = SimpleNamespace(
        aget_state=AsyncMock(return_value=state),
        aupdate_state=AsyncMock(side_effect=update_state),
    )


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
    _wire_content_store(mock_ctx)

    email_data = {
        "id": "SENT_001",
        "subject": "Test sent email",
        "sender": "me@example.com",
        "body": "<p>Hello</p>",
        "attachments": [
            {"name": "file.pdf", "content": "YmFzZTY0ZGF0YQ=="}
        ],
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
async def test_full_pipeline_does_not_upload_attachments_when_feishu_delivery_is_skipped():
    from src.exchange_service import process_and_archive_email

    mock_ctx = MagicMock()
    mock_ctx.db_manager = AsyncMock()
    mock_ctx.db_manager.log_initial_email = AsyncMock(return_value=True)
    mock_ctx.db_manager.update_status = AsyncMock()
    mock_ctx.email_processor = MagicMock()
    mock_ctx.email_processor.process_email = MagicMock()
    mock_ctx.exchange_client = AsyncMock()
    _wire_content_store(mock_ctx)

    email_data = {
        "id": "INBOX_001",
        "subject": "Test incoming email",
        "sender": "someone@example.com",
        "body": "<p>Hello</p>",
        "attachments": [],
        "_event_type": "NewMailEvent",
    }

    with patch(
        "src.exchange_service.decide_notification_kind",
        return_value="skipped",
    ), patch("src.exchange_service._upload_attachments_to_lark") as mock_upload, patch(
        "src.exchange_service._ingest_to_qdrant"
    ) as mock_ingest, patch(
        "src.exchange_service._run_ai_pipeline",
        return_value={"classification": {"need_reply": False}},
    ) as mock_ai, patch(
        "src.exchange_service._dispatch_notification",
        new_callable=AsyncMock,
    ) as mock_notify, patch(
        "src.exchange_service._mark_email_read"
    ) as mock_read:
        mock_notify.return_value = {"delivered": True, "kind": "skipped"}
        await process_and_archive_email(email_data, mock_ctx, skip_analysis=False)

        mock_upload.assert_not_called()
        mock_ingest.assert_called_once()
        mock_ai.assert_called_once()
        mock_notify.assert_called_once()
        mock_read.assert_called_once()


@pytest.mark.asyncio
async def test_read_only_feishu_delivery_uploads_business_attachments():
    from src.exchange_service import process_and_archive_email

    mock_ctx = MagicMock()
    mock_ctx.db_manager = AsyncMock()
    mock_ctx.db_manager.log_initial_email = AsyncMock(return_value=True)
    mock_ctx.db_manager.update_status = AsyncMock()
    mock_ctx.email_processor = MagicMock()
    mock_ctx.email_processor.process_email = MagicMock()
    mock_ctx.exchange_client = AsyncMock()
    _wire_content_store(mock_ctx)

    email_data = {
        "id": "INBOX_READ_ONLY",
        "subject": "Important notice",
        "sender": "someone@example.com",
        "body": "<p>Please read</p>",
        "attachments": [{"name": "notice.pdf", "content": "UERG"}],
        "_event_type": "NewMailEvent",
    }
    pipeline_result = {
        "classification": {
            "need_reply": False,
            "priority": "P1",
            "intent": "通知",
        },
        "email": {**email_data, "attachments": [{"name": "notice.pdf"}]},
    }

    with patch(
        "src.exchange_service.decide_notification_kind",
        return_value="read_only",
    ), patch(
        "src.exchange_service._upload_attachments_to_lark",
        new=AsyncMock(return_value=SimpleNamespace(tokens=(), links=())),
    ) as upload, patch(
        "src.exchange_service._ingest_to_qdrant",
        new_callable=AsyncMock,
    ), patch(
        "src.exchange_service._run_ai_pipeline",
        new=AsyncMock(return_value=pipeline_result),
    ), patch(
        "src.exchange_service._dispatch_notification",
        new=AsyncMock(return_value={"delivered": True, "kind": "read_only"}),
    ), patch(
        "src.exchange_service._mark_email_read",
        new_callable=AsyncMock,
    ):
        await process_and_archive_email(email_data, mock_ctx, skip_analysis=False)

    upload.assert_awaited_once()
