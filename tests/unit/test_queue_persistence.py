from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.storage import ContentRef
from src.domain.email_state import ProcessingOutcome


def _wire_content_store(ctx):
    from src.exchange_service import get_settings

    ref = ContentRef(
        account_id=get_settings().EXCHANGE_ACCOUNT_ID,
        object_id="00000000-0000-4000-8000-000000000047",
        key_version="v1",
        sha256="4" * 64,
    )
    ctx.content_store = AsyncMock()
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


def _empty_upload_projection():
    from src.exchange_service import AttachmentUploadProjection

    return AttachmentUploadProjection(tokens=(), links=())


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
    _wire_content_store(mock_ctx)

    email_data = {
        "id": "fail-test",
        "subject": "Test",
        "body": "Hello",
        "sender": "a@b.com",
        "received_at": "2026-01-01",
    }

    with patch(
        "src.exchange_service._upload_attachments_to_lark",
        new=AsyncMock(return_value=_empty_upload_projection()),
    ), \
         patch("src.exchange_service._ingest_to_qdrant", new_callable=AsyncMock), \
         patch("src.exchange_service._run_ai_pipeline", new_callable=AsyncMock, side_effect=Exception("LLM down")), \
         patch("src.exchange_service._mark_email_read", new_callable=AsyncMock) as mock_mark:

        from src.exchange_service import process_and_archive_email
        outcome = await process_and_archive_email(email_data, mock_ctx)

        mock_mark.assert_not_called()
        assert outcome is ProcessingOutcome.FAILED


@pytest.mark.asyncio
async def test_successful_email_marked_as_read():
    """On success, email should be marked as read."""
    mock_ctx = MagicMock()
    mock_ctx.db_manager = AsyncMock()
    mock_ctx.db_manager.log_initial_email = AsyncMock(return_value=True)
    mock_ctx.db_manager.update_status = AsyncMock()
    _wire_content_store(mock_ctx)

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

    with patch(
        "src.exchange_service._upload_attachments_to_lark",
        new=AsyncMock(return_value=_empty_upload_projection()),
    ), \
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
