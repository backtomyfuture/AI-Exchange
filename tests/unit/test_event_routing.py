import asyncio
from unittest.mock import MagicMock, patch

import pytest


def _make_exchange_client_mock(sentitems_id="SENT_FOLDER_ID", drafts_id="DRAFTS_FOLDER_ID"):
    client = MagicMock()
    client._folder_cache = {
        "INBOX_FOLDER_ID": "Inbox",
        sentitems_id: "Sent Items",
        drafts_id: "Drafts",
        "VIP_FOLDER_ID": "VIP",
        "NOTIF_FOLDER_ID": "通知",
    }
    client.sentitems_folder_id = sentitems_id
    client.drafts_folder_id = drafts_id
    client.get_folder_name = lambda fid: client._folder_cache.get(fid)
    client.get_folder_policy = lambda fid: {
        "INBOX_FOLDER_ID": "full",
        "VIP_FOLDER_ID": "full",
        "NOTIF_FOLDER_ID": "archive",
    }.get(fid, "ignore")
    return client


@pytest.mark.asyncio
async def test_newmail_inbox_full_pipeline():
    from src.exchange_service import enqueue_webhook_event

    mock_ctx = MagicMock()
    mock_ctx.exchange_client = _make_exchange_client_mock()

    with patch("src.exchange_service._worker_ctx", mock_ctx), patch(
        "src.exchange_service._webhook_queue", asyncio.Queue()
    ) as q:
        result = await enqueue_webhook_event(
            {
                "event_type": "NewMailEvent",
                "item_id": {"id": "EMAIL_001", "changekey": "CQ=="},
                "parent_folder_id": {"id": "INBOX_FOLDER_ID", "changekey": "AQ=="},
            }
        )

        assert result["queued"] is True
        assert result["route"] == "full"
        email_data, skip = await q.get()
        assert email_data["id"] == "EMAIL_001"
        assert skip is False


@pytest.mark.asyncio
async def test_newmail_unknown_folder_ignored():
    from src.exchange_service import enqueue_webhook_event

    mock_ctx = MagicMock()
    mock_ctx.exchange_client = _make_exchange_client_mock()

    with patch("src.exchange_service._worker_ctx", mock_ctx), patch(
        "src.exchange_service._webhook_queue", asyncio.Queue()
    ):
        result = await enqueue_webhook_event(
            {
                "event_type": "NewMailEvent",
                "item_id": {"id": "EMAIL_002", "changekey": "CQ=="},
                "parent_folder_id": {"id": "UNKNOWN_FOLDER_ID", "changekey": "AQ=="},
            }
        )

        assert result["queued"] is False
        assert result.get("reason") == "folder_not_in_whitelist"


@pytest.mark.asyncio
async def test_created_sentitems_archive_only():
    from src.exchange_service import enqueue_webhook_event

    mock_ctx = MagicMock()
    mock_ctx.exchange_client = _make_exchange_client_mock()

    with patch("src.exchange_service._worker_ctx", mock_ctx), patch(
        "src.exchange_service._webhook_queue", asyncio.Queue()
    ) as q:
        result = await enqueue_webhook_event(
            {
                "event_type": "CreatedEvent",
                "item_id": {"id": "SENT_EMAIL_001", "changekey": "CQ=="},
                "parent_folder_id": {"id": "SENT_FOLDER_ID", "changekey": "AQ=="},
            }
        )

        assert result["queued"] is True
        assert result["route"] == "archive"
        email_data, skip = await q.get()
        assert email_data["id"] == "SENT_EMAIL_001"
        assert skip is True


@pytest.mark.asyncio
async def test_created_drafts_ignored():
    from src.exchange_service import enqueue_webhook_event

    mock_ctx = MagicMock()
    mock_ctx.exchange_client = _make_exchange_client_mock()

    with patch("src.exchange_service._worker_ctx", mock_ctx), patch(
        "src.exchange_service._webhook_queue", asyncio.Queue()
    ):
        result = await enqueue_webhook_event(
            {
                "event_type": "CreatedEvent",
                "item_id": {"id": "DRAFT_001", "changekey": "CQ=="},
                "parent_folder_id": {"id": "DRAFTS_FOLDER_ID", "changekey": "AQ=="},
            }
        )

        assert result["queued"] is False
        assert result.get("reason") == "drafts_ignored"


@pytest.mark.asyncio
async def test_no_item_id_ignored():
    from src.exchange_service import enqueue_webhook_event

    mock_ctx = MagicMock()
    mock_ctx.exchange_client = _make_exchange_client_mock()

    with patch("src.exchange_service._worker_ctx", mock_ctx), patch(
        "src.exchange_service._webhook_queue", asyncio.Queue()
    ):
        result = await enqueue_webhook_event(
            {
                "event_type": "CreatedEvent",
                "item_id": None,
                "folder_id": {"id": "NEW_FOLDER_ID", "changekey": "AQ=="},
                "parent_folder_id": {"id": "INBOX_FOLDER_ID", "changekey": "AQ=="},
            }
        )

        assert result["queued"] is False
        assert result.get("reason") == "no_item_id"


@pytest.mark.asyncio
async def test_newmail_archive_folder():
    from src.exchange_service import enqueue_webhook_event

    mock_ctx = MagicMock()
    mock_ctx.exchange_client = _make_exchange_client_mock()

    with patch("src.exchange_service._worker_ctx", mock_ctx), patch(
        "src.exchange_service._webhook_queue", asyncio.Queue()
    ) as q:
        result = await enqueue_webhook_event(
            {
                "event_type": "NewMailEvent",
                "item_id": {"id": "NOTIF_001", "changekey": "CQ=="},
                "parent_folder_id": {"id": "NOTIF_FOLDER_ID", "changekey": "AQ=="},
            }
        )

        assert result["queued"] is True
        assert result["route"] == "archive"
        _, skip = await q.get()
        assert skip is True


@pytest.mark.asyncio
async def test_folder_cache_empty_fallback():
    from src.exchange_service import enqueue_webhook_event

    mock_ctx = MagicMock()
    mock_ctx.exchange_client = MagicMock()
    mock_ctx.exchange_client._folder_cache = {}
    mock_ctx.exchange_client._folder_policies = {}
    mock_ctx.exchange_client.sentitems_folder_id = None
    mock_ctx.exchange_client.drafts_folder_id = None
    mock_ctx.exchange_client.get_folder_name = lambda fid: None
    mock_ctx.exchange_client.get_folder_policy = lambda fid: "ignore"

    with patch("src.exchange_service._worker_ctx", mock_ctx), patch(
        "src.exchange_service._webhook_queue", asyncio.Queue()
    ):
        result = await enqueue_webhook_event(
            {
                "event_type": "NewMailEvent",
                "item_id": {"id": "EMAIL_003", "changekey": "CQ=="},
                "parent_folder_id": {"id": "UNKNOWN_ID", "changekey": "AQ=="},
            }
        )

        assert result["queued"] is True
        assert result["route"] == "full"
