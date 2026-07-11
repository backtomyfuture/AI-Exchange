from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

import psycopg
import pytest

from src.domain.errors import DatabaseOperationError
from src.utils.exchange_api import ExchangeClient
from src.utils.db_async import AsyncDatabaseManager
from src.utils.lark_file_ops import delete_file_from_drive, upload_file_to_drive
from src.utils.lark_messaging import send_approval_card


SECRET = "BOUNDARY-SECRET-SENTINEL"


def test_lark_card_failures_do_not_log_card_or_sdk_error_text(caplog):
    settings = SimpleNamespace(LARK_CHAT_ID="chat-id")
    client = MagicMock()
    response = client.im.v1.message.create.return_value
    response.success.return_value = False
    response.code = 500
    response.msg = SECRET
    response.error = SECRET
    card_builder = MagicMock()
    card_builder.build_approval_card.return_value = {
        "body": SECRET,
    }

    with patch("src.utils.lark_messaging.get_settings", return_value=settings):
        assert not send_approval_card(
            "mail-1",
            SECRET,
            [],
            {"body": SECRET},
            {},
            lark_api_client=client,
            card_builder=card_builder,
        )

    assert SECRET not in caplog.text
    assert "code=500" in caplog.text


def test_lark_drive_failures_do_not_log_names_tokens_or_sdk_text(caplog):
    settings = SimpleNamespace(LARK_DRIVE_FOLDER_TOKEN="folder-token")
    client = MagicMock()
    upload_response = client.drive.v1.file.upload_all.return_value
    upload_response.success.return_value = False
    upload_response.code = 500
    upload_response.msg = SECRET
    delete_response = client.drive.v1.file.delete.return_value
    delete_response.success.return_value = False
    delete_response.code = 501
    delete_response.msg = SECRET

    with patch("src.utils.lark_file_ops.get_settings", return_value=settings):
        assert upload_file_to_drive(
            f"{SECRET}.pdf",
            SECRET.encode(),
            len(SECRET),
            lark_api_client=client,
        ) is None
        assert not delete_file_from_drive(SECRET, lark_api_client=client)

    assert SECRET not in caplog.text
    assert "code=500" in caplog.text
    assert "code=501" in caplog.text


@pytest.mark.asyncio
async def test_exchange_send_boundaries_never_log_response_or_exception_text(
    mock_settings,
    caplog,
):
    client = ExchangeClient(settings=mock_settings)
    http = AsyncMock()
    response = MagicMock()
    response.status_code = 500
    response.text = SECRET
    response.raise_for_status.side_effect = RuntimeError(SECRET)
    http.post.return_value = response

    with patch.object(
        type(client),
        "http_client",
        new_callable=PropertyMock,
        return_value=http,
    ):
        assert not await client.reply_email("mail-1", SECRET)
        assert not await client.forward_email("mail-1", ["to@example.com"], SECRET)
        assert not await client.create_draft(["to@example.com"], SECRET, SECRET)

    assert SECRET not in caplog.text
    assert "status=500" in caplog.text


@pytest.mark.asyncio
async def test_draft_status_database_failure_does_not_log_content(caplog):
    manager = AsyncDatabaseManager(
        MagicMock(database_url="postgresql://test/test")
    )

    @asynccontextmanager
    async def failing_connection():
        raise psycopg.OperationalError(SECRET)
        yield

    manager.get_connection = failing_connection

    with pytest.raises(DatabaseOperationError) as caught:
        await manager.update_status(
            "mail-1",
            "approved",
            final_draft=SECRET,
        )

    assert caught.value.__cause__ is None
    assert caught.value.__suppress_context__ is True
    assert SECRET not in str(caught.value)
    assert SECRET not in caplog.text
