"""Unit tests for ``src.utils.lark_pdf_flow``."""

from unittest.mock import MagicMock, patch

import pytest

from src.utils import lark_pdf_flow


@pytest.mark.asyncio
async def test_generate_returns_url_and_token_on_success():
    upload = MagicMock(return_value={"url": "u", "file_token": "tok"})
    with patch.object(lark_pdf_flow, "render_email_html", return_value="<html/>"), \
         patch.object(lark_pdf_flow, "convert_html_to_pdf", return_value=b"PDF"):
        result = await lark_pdf_flow.generate_and_upload_pdf(
            "msg-1", {"subject": "s"}, upload_fn=upload
        )
    assert result == {"url": "u", "file_token": "tok"}
    upload.assert_called_once()
    args, _ = upload.call_args
    assert args[0] == "Email_Export_msg-1.pdf"
    assert args[1] == b"PDF"
    assert args[2] == 3


@pytest.mark.asyncio
async def test_generate_returns_none_when_pdf_empty():
    with patch.object(lark_pdf_flow, "render_email_html", return_value="<html/>"), \
         patch.object(lark_pdf_flow, "convert_html_to_pdf", return_value=b""):
        result = await lark_pdf_flow.generate_and_upload_pdf(
            "msg-1", {"subject": "s"}, upload_fn=MagicMock()
        )
    assert result is None


@pytest.mark.asyncio
async def test_generate_returns_none_when_upload_fails():
    upload = MagicMock(return_value=None)
    with patch.object(lark_pdf_flow, "render_email_html", return_value="<html/>"), \
         patch.object(lark_pdf_flow, "convert_html_to_pdf", return_value=b"PDF"):
        result = await lark_pdf_flow.generate_and_upload_pdf(
            "msg-1", {"subject": "s"}, upload_fn=upload
        )
    assert result is None


@pytest.mark.asyncio
async def test_generate_returns_none_when_pdf_conversion_raises():
    def explode(*_a, **_k):
        raise RuntimeError("bad pdf")
    with patch.object(lark_pdf_flow, "render_email_html", return_value="<html/>"), \
         patch.object(lark_pdf_flow, "convert_html_to_pdf", side_effect=explode):
        result = await lark_pdf_flow.generate_and_upload_pdf(
            "msg-1", {"subject": "s"}, upload_fn=MagicMock()
        )
    assert result is None


@pytest.mark.asyncio
async def test_process_pdf_skips_reply_when_pdf_generation_fails():
    fake_state = MagicMock()
    fake_state.values = {"email": {"subject": "s"}}
    fake_graph = MagicMock()
    fake_lark = MagicMock()
    safe_async_wait = MagicMock()
    upload = MagicMock(return_value=None)

    with patch.object(lark_pdf_flow, "render_email_html", return_value="<html/>"), \
         patch.object(lark_pdf_flow, "convert_html_to_pdf", return_value=b"PDF"):
        await lark_pdf_flow.process_pdf_generation_and_reply(
            "msg-1",
            fake_state,
            "msg_x",
            graph=fake_graph,
            lark_api_client=fake_lark,
            upload_fn=upload,
            safe_async_wait=safe_async_wait,
        )

    fake_lark.im.v1.message.reply.assert_not_called()
    safe_async_wait.assert_not_called()


@pytest.mark.asyncio
async def test_process_pdf_persists_token_and_replies_on_success():
    fake_state = MagicMock()
    fake_state.values = {"email": {"subject": "s"}}
    fake_graph = MagicMock()
    fake_lark = MagicMock()
    safe_async_wait = MagicMock()
    upload = MagicMock(return_value={"url": "URL", "file_token": "TOK"})

    with patch.object(lark_pdf_flow, "render_email_html", return_value="<html/>"), \
         patch.object(lark_pdf_flow, "convert_html_to_pdf", return_value=b"PDF"):
        await lark_pdf_flow.process_pdf_generation_and_reply(
            "msg-1",
            fake_state,
            "msg_x",
            graph=fake_graph,
            lark_api_client=fake_lark,
            upload_fn=upload,
            safe_async_wait=safe_async_wait,
        )

    safe_async_wait.assert_called_once()
    fake_graph.aupdate_state.assert_called_once()
    fake_lark.im.v1.message.reply.assert_called_once()


@pytest.mark.asyncio
async def test_process_pdf_skips_reply_when_lark_client_missing():
    fake_state = MagicMock()
    fake_state.values = {"email": {"subject": "s"}}
    fake_graph = MagicMock()
    safe_async_wait = MagicMock()
    upload = MagicMock(return_value={"url": "URL", "file_token": "TOK"})

    with patch.object(lark_pdf_flow, "render_email_html", return_value="<html/>"), \
         patch.object(lark_pdf_flow, "convert_html_to_pdf", return_value=b"PDF"):
        await lark_pdf_flow.process_pdf_generation_and_reply(
            "msg-1",
            fake_state,
            "msg_x",
            graph=fake_graph,
            lark_api_client=None,
            upload_fn=upload,
            safe_async_wait=safe_async_wait,
        )

    # graph state is still persisted even though we cannot reply.
    safe_async_wait.assert_called_once()
