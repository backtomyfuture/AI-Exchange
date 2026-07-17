"""Unit tests for ``src.utils.lark_pdf_flow``."""

import asyncio
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

from src.utils import lark_pdf_flow
from src.graph.dependencies import GraphDependencies
from src.graph.state_factory import build_initial_graph_state
from src.storage import ContentRef


def _ref():
    return ContentRef(
        account_id=8,
        object_id="00000000-0000-4000-8000-000000000077",
        key_version="v1",
        sha256="7" * 64,
    )


def _pdf_boundary(monkeypatch, *, old_token=None):
    monkeypatch.setattr(
        "src.graph.state_factory.get_settings",
        lambda: SimpleNamespace(EXCHANGE_ACCOUNT_ID=8),
    )
    email = {
        "id": "msg-1",
        "subject": "subject",
        "body": "PDF-BODY-SENTINEL",
        "attachments": [
            {"name": "inline.png", "content": "BASE64-INLINE-SENTINEL"}
        ],
    }
    store = SimpleNamespace(load_email=AsyncMock(return_value=deepcopy(email)))
    drafts = SimpleNamespace()
    dependencies = GraphDependencies(content_store=store, drafts=drafts)
    values = build_initial_graph_state(email, _ref())
    values["pdf_token"] = old_token
    state = SimpleNamespace(values=values)
    return state, dependencies, store


def _successful_lark_client():
    client = MagicMock()
    client.im.v1.message.reply.return_value.success.return_value = True
    return client


@pytest.mark.asyncio
async def test_pdf_renderer_hydrates_attachment_bytes_only_at_edge(monkeypatch):
    state, dependencies, store = _pdf_boundary(monkeypatch)
    captured = {}

    def render(email):
        captured["email"] = email
        return "<html/>"

    with patch.object(lark_pdf_flow, "render_email_html", side_effect=render), patch.object(
        lark_pdf_flow, "convert_html_to_pdf", return_value=b"PDF"
    ):
        result = await lark_pdf_flow.generate_and_upload_pdf(
            "msg-1",
            state,
            dependencies=dependencies,
            upload_fn=MagicMock(return_value={"url": "URL", "file_token": "TOK"}),
        )

    assert result == {"url": "URL", "file_token": "TOK"}
    assert captured["email"]["attachments"][0]["content"] == (
        "BASE64-INLINE-SENTINEL"
    )
    assert "PDF-BODY-SENTINEL" not in str(state.values)
    assert store.load_email.await_args.kwargs == {"include_attachments": True}


@pytest.mark.asyncio
async def test_pdf_state_write_failure_deletes_new_remote_token(monkeypatch):
    state, dependencies, _store = _pdf_boundary(monkeypatch)
    graph = SimpleNamespace(
        aupdate_state=AsyncMock(side_effect=RuntimeError("write failed"))
    )
    delete = MagicMock(return_value=True)

    with patch.object(lark_pdf_flow, "render_email_html", return_value="<html/>"), patch.object(
        lark_pdf_flow, "convert_html_to_pdf", return_value=b"PDF"
    ):
        await lark_pdf_flow.process_pdf_generation_and_reply(
            "msg-1",
            state,
            "msg_x",
            graph=graph,
            dependencies=dependencies,
            lark_api_client=MagicMock(),
            upload_fn=MagicMock(return_value={"url": "URL", "file_token": "NEW"}),
            delete_fn=delete,
        )

    delete.assert_called_once_with("NEW")


@pytest.mark.asyncio
async def test_pdf_state_write_failure_retains_new_handle_when_delete_fails(monkeypatch):
    state, dependencies, _store = _pdf_boundary(monkeypatch)
    graph = SimpleNamespace(
        aupdate_state=AsyncMock(side_effect=[RuntimeError("write failed"), None])
    )
    delete = MagicMock(return_value=False)

    with patch.object(lark_pdf_flow, "render_email_html", return_value="<html/>"), patch.object(
        lark_pdf_flow, "convert_html_to_pdf", return_value=b"PDF"
    ):
        await lark_pdf_flow.process_pdf_generation_and_reply(
            "msg-1",
            state,
            "msg_x",
            graph=graph,
            dependencies=dependencies,
            lark_api_client=MagicMock(),
            upload_fn=MagicMock(return_value={"url": "URL", "file_token": "NEW"}),
            delete_fn=delete,
        )

    assert graph.aupdate_state.await_args_list[-1].args[1] == {
        "attachment_tokens": ["NEW"]
    }


@pytest.mark.asyncio
async def test_pdf_replacement_deletes_old_token_after_success(monkeypatch):
    state, dependencies, _store = _pdf_boundary(monkeypatch, old_token="OLD")
    state.values["attachment_tokens"] = ["KEEP"]
    graph = SimpleNamespace(aupdate_state=AsyncMock())
    delete = MagicMock(return_value=True)

    lark_client = MagicMock()
    lark_client.im.v1.message.reply.return_value.success.return_value = True
    with patch.object(lark_pdf_flow, "render_email_html", return_value="<html/>"), patch.object(
        lark_pdf_flow, "convert_html_to_pdf", return_value=b"PDF"
    ):
        await lark_pdf_flow.process_pdf_generation_and_reply(
            "msg-1",
            state,
            "msg_x",
            graph=graph,
            dependencies=dependencies,
            lark_api_client=lark_client,
            upload_fn=MagicMock(return_value={"url": "URL", "file_token": "NEW"}),
            delete_fn=delete,
        )

    assert graph.aupdate_state.await_args_list[0].args[1] == {
        "pdf_token": "NEW",
        "attachment_tokens": ["KEEP", "OLD"],
    }
    assert graph.aupdate_state.await_args_list[1].args[1] == {
        "attachment_tokens": ["KEEP"]
    }
    delete.assert_called_once_with("OLD")


@pytest.mark.asyncio
async def test_pdf_replacement_retains_old_handle_when_delete_returns_false(monkeypatch):
    state, dependencies, _store = _pdf_boundary(monkeypatch, old_token="OLD")
    graph = SimpleNamespace(aupdate_state=AsyncMock())
    delete = MagicMock(return_value=False)
    lark_client = MagicMock()
    lark_client.im.v1.message.reply.return_value.success.return_value = True

    with patch.object(lark_pdf_flow, "render_email_html", return_value="<html/>"), patch.object(
        lark_pdf_flow, "convert_html_to_pdf", return_value=b"PDF"
    ):
        await lark_pdf_flow.process_pdf_generation_and_reply(
            "msg-1",
            state,
            "msg_x",
            graph=graph,
            dependencies=dependencies,
            lark_api_client=lark_client,
            upload_fn=MagicMock(return_value={"url": "URL", "file_token": "NEW"}),
            delete_fn=delete,
        )

    assert any(
        call.args[1].get("attachment_tokens") == ["OLD"]
        for call in graph.aupdate_state.await_args_list
    )


@pytest.mark.asyncio
async def test_pdf_invalid_uploaded_token_is_deleted(monkeypatch):
    state, dependencies, _store = _pdf_boundary(monkeypatch)
    delete = MagicMock(return_value=True)

    with patch.object(lark_pdf_flow, "render_email_html", return_value="<html/>"), patch.object(
        lark_pdf_flow, "convert_html_to_pdf", return_value=b"PDF"
    ):
        result = await lark_pdf_flow.generate_and_upload_pdf(
            "msg-1",
            state,
            dependencies=dependencies,
            upload_fn=MagicMock(
                return_value={"url": "URL", "file_token": "x" * 513}
            ),
            delete_fn=delete,
        )

    assert result is None
    delete.assert_called_once_with("x" * 513)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "delete_effect",
    [False, RuntimeError("delete failed")],
    ids=["delete-returned-false", "delete-raised"],
)
async def test_pdf_invalid_uploaded_token_returns_cleanup_handle_when_delete_is_inconclusive(
    monkeypatch,
    delete_effect,
):
    state, dependencies, _store = _pdf_boundary(monkeypatch)
    invalid_token = "x" * 513
    delete = MagicMock()
    if isinstance(delete_effect, Exception):
        delete.side_effect = delete_effect
    else:
        delete.return_value = delete_effect

    with patch.object(lark_pdf_flow, "render_email_html", return_value="<html/>"), patch.object(
        lark_pdf_flow, "convert_html_to_pdf", return_value=b"PDF"
    ):
        result = await lark_pdf_flow.generate_and_upload_pdf(
            "msg-1",
            state,
            dependencies=dependencies,
            upload_fn=MagicMock(
                return_value={"url": "URL", "file_token": invalid_token}
            ),
            delete_fn=delete,
        )

    assert result.status == "upload_invalid_cleanup_required"
    assert result.cleanup_tokens == (invalid_token,)
    assert result.retryable is True
    delete.assert_called_once_with(invalid_token)


@pytest.mark.asyncio
async def test_pdf_invalid_response_does_not_delete_an_existing_state_handle(
    monkeypatch,
):
    state, dependencies, _store = _pdf_boundary(monkeypatch, old_token="OLD")
    delete = MagicMock(return_value=True)

    with patch.object(lark_pdf_flow, "render_email_html", return_value="<html/>"), patch.object(
        lark_pdf_flow, "convert_html_to_pdf", return_value=b"PDF"
    ):
        result = await lark_pdf_flow.generate_and_upload_pdf(
            "msg-1",
            state,
            dependencies=dependencies,
            upload_fn=MagicMock(
                return_value={"url": "", "file_token": "OLD"}
            ),
            delete_fn=delete,
        )

    assert result.status == "upload_invalid_protected_token"
    assert result.protected_tokens == ("OLD",)
    assert result.retryable is True
    delete.assert_not_called()


@pytest.mark.asyncio
async def test_pdf_reply_failure_restores_old_token_and_deletes_new(monkeypatch):
    state, dependencies, _store = _pdf_boundary(monkeypatch, old_token="OLD")
    state.values["attachment_tokens"] = ["KEEP"]
    graph = SimpleNamespace(aupdate_state=AsyncMock())
    lark_client = MagicMock()
    lark_client.im.v1.message.reply.side_effect = RuntimeError("reply failed")
    delete = MagicMock(return_value=True)

    with patch.object(lark_pdf_flow, "render_email_html", return_value="<html/>"), patch.object(
        lark_pdf_flow, "convert_html_to_pdf", return_value=b"PDF"
    ):
        await lark_pdf_flow.process_pdf_generation_and_reply(
            "msg-1",
            state,
            "msg_x",
            graph=graph,
            dependencies=dependencies,
            lark_api_client=lark_client,
            upload_fn=MagicMock(return_value={"url": "URL", "file_token": "NEW"}),
            delete_fn=delete,
        )

    assert graph.aupdate_state.await_args_list[0].args[1] == {
        "pdf_token": "NEW",
        "attachment_tokens": ["KEEP", "OLD"],
    }
    assert graph.aupdate_state.await_args_list[1].args[1] == {
        "pdf_token": "OLD",
        "attachment_tokens": ["KEEP", "NEW"],
    }
    assert graph.aupdate_state.await_args_list[2].args[1] == {
        "attachment_tokens": ["KEEP"]
    }
    delete.assert_called_once_with("NEW")


@pytest.mark.asyncio
async def test_pdf_reply_failure_retains_new_handle_when_delete_fails(monkeypatch):
    state, dependencies, _store = _pdf_boundary(monkeypatch, old_token="OLD")
    graph = SimpleNamespace(aupdate_state=AsyncMock())
    lark_client = MagicMock()
    lark_client.im.v1.message.reply.side_effect = RuntimeError("reply failed")
    delete = MagicMock(return_value=False)

    with patch.object(lark_pdf_flow, "render_email_html", return_value="<html/>"), patch.object(
        lark_pdf_flow, "convert_html_to_pdf", return_value=b"PDF"
    ):
        await lark_pdf_flow.process_pdf_generation_and_reply(
            "msg-1",
            state,
            "msg_x",
            graph=graph,
            dependencies=dependencies,
            lark_api_client=lark_client,
            upload_fn=MagicMock(return_value={"url": "URL", "file_token": "NEW"}),
            delete_fn=delete,
        )

    assert graph.aupdate_state.await_args_list[-1].args[1] == {
        "pdf_token": "OLD",
        "attachment_tokens": ["NEW"],
    }


@pytest.mark.asyncio
async def test_pdf_state_write_failure_reports_untracked_cleanup_when_retention_write_fails(
    monkeypatch,
):
    state, dependencies, _store = _pdf_boundary(monkeypatch)
    state.values["attachment_tokens"] = ["KEEP"]
    graph = SimpleNamespace(
        aupdate_state=AsyncMock(
            side_effect=[RuntimeError("write failed"), RuntimeError("retain failed")]
        )
    )
    delete = MagicMock(return_value=False)

    with patch.object(lark_pdf_flow, "render_email_html", return_value="<html/>"), patch.object(
        lark_pdf_flow, "convert_html_to_pdf", return_value=b"PDF"
    ):
        result = await lark_pdf_flow.process_pdf_generation_and_reply(
            "msg-1",
            state,
            "msg_x",
            graph=graph,
            dependencies=dependencies,
            lark_api_client=MagicMock(),
            upload_fn=MagicMock(return_value={"url": "URL", "file_token": "NEW"}),
            delete_fn=delete,
        )

    assert result.status == "state_write_failed_cleanup_untracked"
    assert result.cleanup_tokens == ("NEW",)
    assert result.retryable is True
    assert graph.aupdate_state.await_args_list[-1].args[1] == {
        "attachment_tokens": ["KEEP", "NEW"]
    }


@pytest.mark.asyncio
async def test_pdf_state_write_ack_failure_keeps_referenced_new_token(monkeypatch):
    state, dependencies, _store = _pdf_boundary(monkeypatch, old_token="OLD")
    current_values = deepcopy(state.values)
    current_values["pdf_token"] = "NEW"
    current_values["attachment_tokens"] = ["OLD"]
    graph = SimpleNamespace(
        aupdate_state=AsyncMock(side_effect=[RuntimeError("ack lost"), None]),
        aget_state=AsyncMock(
            side_effect=[
                SimpleNamespace(values=deepcopy(state.values)),
                SimpleNamespace(values=current_values),
                SimpleNamespace(values=current_values),
                SimpleNamespace(values=current_values),
            ]
        ),
    )
    delete = MagicMock(return_value=True)

    with patch.object(lark_pdf_flow, "render_email_html", return_value="<html/>"), patch.object(
        lark_pdf_flow, "convert_html_to_pdf", return_value=b"PDF"
    ):
        result = await lark_pdf_flow.process_pdf_generation_and_reply(
            "msg-1",
            state,
            "msg_x",
            graph=graph,
            dependencies=dependencies,
            lark_api_client=_successful_lark_client(),
            upload_fn=MagicMock(return_value={"url": "URL", "file_token": "NEW"}),
            delete_fn=delete,
        )

    assert result.status == "reply_sent"
    assert result.cleanup_tokens == ()
    delete.assert_called_once_with("OLD")
    assert call("NEW") not in delete.call_args_list


@pytest.mark.asyncio
async def test_pdf_state_write_ack_readback_requires_old_cleanup_registration(
    monkeypatch,
):
    state, dependencies, _store = _pdf_boundary(monkeypatch, old_token="OLD")
    partial_values = deepcopy(state.values)
    partial_values["pdf_token"] = "NEW"
    graph = SimpleNamespace(
        aupdate_state=AsyncMock(side_effect=RuntimeError("ack lost")),
        aget_state=AsyncMock(
            side_effect=[
                SimpleNamespace(values=deepcopy(state.values)),
                SimpleNamespace(values=partial_values),
            ]
        ),
    )
    delete = MagicMock(return_value=True)
    lark_client = _successful_lark_client()

    with patch.object(lark_pdf_flow, "render_email_html", return_value="<html/>"), patch.object(
        lark_pdf_flow, "convert_html_to_pdf", return_value=b"PDF"
    ):
        result = await lark_pdf_flow.process_pdf_generation_and_reply(
            "msg-1",
            state,
            "msg_x",
            graph=graph,
            dependencies=dependencies,
            lark_api_client=lark_client,
            upload_fn=MagicMock(return_value={"url": "URL", "file_token": "NEW"}),
            delete_fn=delete,
        )

    assert result.status == "state_write_ambiguous"
    assert result.protected_tokens == ("OLD", "NEW")
    lark_client.im.v1.message.reply.assert_not_called()
    delete.assert_not_called()


@pytest.mark.asyncio
async def test_pdf_replacement_fails_closed_when_cleanup_handle_list_is_full(
    monkeypatch,
):
    state, dependencies, _store = _pdf_boundary(monkeypatch, old_token="OLD")
    state.values["attachment_tokens"] = [f"KEEP-{index}" for index in range(32)]
    graph = SimpleNamespace(aupdate_state=AsyncMock())
    delete = MagicMock(return_value=False)
    lark_client = _successful_lark_client()

    with patch.object(lark_pdf_flow, "render_email_html", return_value="<html/>"), patch.object(
        lark_pdf_flow, "convert_html_to_pdf", return_value=b"PDF"
    ):
        result = await lark_pdf_flow.process_pdf_generation_and_reply(
            "msg-1",
            state,
            "msg_x",
            graph=graph,
            dependencies=dependencies,
            lark_api_client=lark_client,
            upload_fn=MagicMock(return_value={"url": "URL", "file_token": "NEW"}),
            delete_fn=delete,
        )

    assert result.status == "state_precondition_failed_cleanup_untracked"
    assert result.cleanup_tokens == ("NEW",)
    assert result.protected_tokens == ("OLD",)
    graph.aupdate_state.assert_not_awaited()
    lark_client.im.v1.message.reply.assert_not_called()


@pytest.mark.asyncio
async def test_pdf_precondition_read_failure_cleans_unreferenced_new(
    monkeypatch,
):
    state, dependencies, _store = _pdf_boundary(monkeypatch, old_token="OLD")
    graph = SimpleNamespace(
        aupdate_state=AsyncMock(side_effect=RuntimeError("write failed")),
        aget_state=AsyncMock(side_effect=RuntimeError("readback failed")),
    )
    delete = MagicMock(return_value=True)

    with patch.object(lark_pdf_flow, "render_email_html", return_value="<html/>"), patch.object(
        lark_pdf_flow, "convert_html_to_pdf", return_value=b"PDF"
    ):
        result = await lark_pdf_flow.process_pdf_generation_and_reply(
            "msg-1",
            state,
            "msg_x",
            graph=graph,
            dependencies=dependencies,
            lark_api_client=_successful_lark_client(),
            upload_fn=MagicMock(return_value={"url": "URL", "file_token": "NEW"}),
            delete_fn=delete,
        )

    assert result.status == "state_precondition_read_failed_cleaned"
    assert result.protected_tokens == ("OLD",)
    assert result.retryable is True
    delete.assert_called_once_with("NEW")


@pytest.mark.asyncio
async def test_pdf_empty_latest_graph_state_fails_closed_before_transition(
    monkeypatch,
):
    state, dependencies, _store = _pdf_boundary(monkeypatch, old_token="OLD")
    graph = SimpleNamespace(
        aupdate_state=AsyncMock(),
        aget_state=AsyncMock(return_value=SimpleNamespace(values={})),
    )
    delete = MagicMock(return_value=True)
    lark_client = _successful_lark_client()

    with patch.object(lark_pdf_flow, "render_email_html", return_value="<html/>"), patch.object(
        lark_pdf_flow, "convert_html_to_pdf", return_value=b"PDF"
    ):
        result = await lark_pdf_flow.process_pdf_generation_and_reply(
            "msg-1",
            state,
            "msg_x",
            graph=graph,
            dependencies=dependencies,
            lark_api_client=lark_client,
            upload_fn=MagicMock(return_value={"url": "URL", "file_token": "NEW"}),
            delete_fn=delete,
        )

    assert result.status == "state_precondition_read_failed_cleaned"
    assert result.protected_tokens == ("OLD",)
    graph.aupdate_state.assert_not_awaited()
    lark_client.im.v1.message.reply.assert_not_called()
    delete.assert_called_once_with("NEW")


@pytest.mark.asyncio
async def test_pdf_reply_failure_reports_restore_failure_without_deleting_new(
    monkeypatch,
):
    state, dependencies, _store = _pdf_boundary(monkeypatch, old_token="OLD")
    graph = SimpleNamespace(
        aupdate_state=AsyncMock(side_effect=[None, RuntimeError("restore failed")])
    )
    lark_client = MagicMock()
    lark_client.im.v1.message.reply.side_effect = RuntimeError("reply failed")
    delete = MagicMock(return_value=True)

    with patch.object(lark_pdf_flow, "render_email_html", return_value="<html/>"), patch.object(
        lark_pdf_flow, "convert_html_to_pdf", return_value=b"PDF"
    ):
        result = await lark_pdf_flow.process_pdf_generation_and_reply(
            "msg-1",
            state,
            "msg_x",
            graph=graph,
            dependencies=dependencies,
            lark_api_client=lark_client,
            upload_fn=MagicMock(return_value={"url": "URL", "file_token": "NEW"}),
            delete_fn=delete,
        )

    assert result.status == "reply_failed_restore_failed"
    assert result.protected_tokens == ("OLD", "NEW")
    assert result.retryable is True
    delete.assert_not_called()


@pytest.mark.asyncio
async def test_pdf_reply_failure_reconciles_a_committed_restore_before_cleanup(
    monkeypatch,
):
    state, dependencies, _store = _pdf_boundary(monkeypatch, old_token="OLD")
    staged_values = deepcopy(state.values)
    staged_values["pdf_token"] = "NEW"
    staged_values["attachment_tokens"] = ["OLD"]
    restored_values = deepcopy(state.values)
    restored_values["attachment_tokens"] = ["NEW"]
    graph = SimpleNamespace(
        aupdate_state=AsyncMock(
            side_effect=[None, RuntimeError("restore ack lost")]
        ),
        aget_state=AsyncMock(
            side_effect=[
                SimpleNamespace(values=deepcopy(state.values)),
                SimpleNamespace(values=staged_values),
                SimpleNamespace(values=restored_values),
                SimpleNamespace(values=restored_values),
            ]
        ),
    )
    lark_client = MagicMock()
    lark_client.im.v1.message.reply.side_effect = RuntimeError("reply failed")

    with patch.object(lark_pdf_flow, "render_email_html", return_value="<html/>"), patch.object(
        lark_pdf_flow, "convert_html_to_pdf", return_value=b"PDF"
    ):
        result = await lark_pdf_flow.process_pdf_generation_and_reply(
            "msg-1",
            state,
            "msg_x",
            graph=graph,
            dependencies=dependencies,
            lark_api_client=lark_client,
            upload_fn=MagicMock(return_value={"url": "URL", "file_token": "NEW"}),
            delete_fn=MagicMock(return_value=False),
        )

    assert result.status == "reply_failed_cleanup_pending"
    assert result.cleanup_tokens == ("NEW",)
    assert graph.aupdate_state.await_args_list[-1].args[1] == {
        "pdf_token": "OLD",
        "attachment_tokens": ["NEW"],
    }


@pytest.mark.asyncio
async def test_pdf_restore_ack_readback_requires_new_cleanup_registration(
    monkeypatch,
):
    state, dependencies, _store = _pdf_boundary(monkeypatch, old_token="OLD")
    partial_restore = deepcopy(state.values)
    graph = SimpleNamespace(
        aupdate_state=AsyncMock(
            side_effect=[None, RuntimeError("restore ack lost")]
        ),
        aget_state=AsyncMock(return_value=SimpleNamespace(values=partial_restore)),
    )
    lark_client = MagicMock()
    lark_client.im.v1.message.reply.side_effect = RuntimeError("reply failed")
    delete = MagicMock(return_value=True)

    with patch.object(lark_pdf_flow, "render_email_html", return_value="<html/>"), patch.object(
        lark_pdf_flow, "convert_html_to_pdf", return_value=b"PDF"
    ):
        result = await lark_pdf_flow.process_pdf_generation_and_reply(
            "msg-1",
            state,
            "msg_x",
            graph=graph,
            dependencies=dependencies,
            lark_api_client=lark_client,
            upload_fn=MagicMock(return_value={"url": "URL", "file_token": "NEW"}),
            delete_fn=delete,
        )

    assert result.status == "reply_failed_restore_ambiguous"
    assert result.protected_tokens == ("OLD", "NEW")
    delete.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "delete_effect",
    [False, RuntimeError("delete failed")],
    ids=["delete-returned-false", "delete-raised"],
)
async def test_pdf_reply_failure_restores_old_and_tracks_new_when_delete_is_inconclusive(
    monkeypatch,
    delete_effect,
):
    state, dependencies, _store = _pdf_boundary(monkeypatch, old_token="OLD")
    state.values["attachment_tokens"] = ["KEEP"]
    graph = SimpleNamespace(aupdate_state=AsyncMock())
    lark_client = MagicMock()
    lark_client.im.v1.message.reply.side_effect = RuntimeError("reply failed")
    delete = MagicMock()
    if isinstance(delete_effect, Exception):
        delete.side_effect = delete_effect
    else:
        delete.return_value = delete_effect

    with patch.object(lark_pdf_flow, "render_email_html", return_value="<html/>"), patch.object(
        lark_pdf_flow, "convert_html_to_pdf", return_value=b"PDF"
    ):
        result = await lark_pdf_flow.process_pdf_generation_and_reply(
            "msg-1",
            state,
            "msg_x",
            graph=graph,
            dependencies=dependencies,
            lark_api_client=lark_client,
            upload_fn=MagicMock(return_value={"url": "URL", "file_token": "NEW"}),
            delete_fn=delete,
        )

    assert result.status == "reply_failed_cleanup_pending"
    assert result.cleanup_tokens == ("NEW",)
    assert result.retryable is True
    assert graph.aupdate_state.await_args_list[-1].args[1] == {
        "pdf_token": "OLD",
        "attachment_tokens": ["KEEP", "NEW"],
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "delete_effect",
    [False, RuntimeError("delete failed")],
    ids=["delete-returned-false", "delete-raised"],
)
async def test_pdf_replacement_tracks_old_when_delete_is_inconclusive(
    monkeypatch,
    delete_effect,
):
    state, dependencies, _store = _pdf_boundary(monkeypatch, old_token="OLD")
    state.values["attachment_tokens"] = ["KEEP"]
    graph = SimpleNamespace(aupdate_state=AsyncMock())
    delete = MagicMock()
    if isinstance(delete_effect, Exception):
        delete.side_effect = delete_effect
    else:
        delete.return_value = delete_effect

    with patch.object(lark_pdf_flow, "render_email_html", return_value="<html/>"), patch.object(
        lark_pdf_flow, "convert_html_to_pdf", return_value=b"PDF"
    ):
        result = await lark_pdf_flow.process_pdf_generation_and_reply(
            "msg-1",
            state,
            "msg_x",
            graph=graph,
            dependencies=dependencies,
            lark_api_client=_successful_lark_client(),
            upload_fn=MagicMock(return_value={"url": "URL", "file_token": "NEW"}),
            delete_fn=delete,
        )

    assert result.status == "reply_sent_cleanup_pending"
    assert result.cleanup_tokens == ("OLD",)
    assert result.reply_sent is True
    assert graph.aupdate_state.await_args_list[-1].args[1] == {
        "pdf_token": "NEW",
        "attachment_tokens": ["KEEP", "OLD"],
    }


@pytest.mark.asyncio
async def test_pdf_replacement_merges_latest_cleanup_handles_before_retaining_old(
    monkeypatch,
):
    state, dependencies, _store = _pdf_boundary(monkeypatch, old_token="OLD")
    state.values["attachment_tokens"] = ["KEEP"]
    latest_values = deepcopy(state.values)
    latest_values["attachment_tokens"] = ["KEEP", "CONCURRENT"]
    staged_values = deepcopy(latest_values)
    staged_values["pdf_token"] = "NEW"
    staged_values["attachment_tokens"] = ["KEEP", "CONCURRENT", "OLD"]
    graph = SimpleNamespace(
        aupdate_state=AsyncMock(),
        aget_state=AsyncMock(
            side_effect=[
                SimpleNamespace(values=latest_values),
                SimpleNamespace(values=staged_values),
            ]
        ),
    )

    with patch.object(lark_pdf_flow, "render_email_html", return_value="<html/>"), patch.object(
        lark_pdf_flow, "convert_html_to_pdf", return_value=b"PDF"
    ):
        result = await lark_pdf_flow.process_pdf_generation_and_reply(
            "msg-1",
            state,
            "msg_x",
            graph=graph,
            dependencies=dependencies,
            lark_api_client=_successful_lark_client(),
            upload_fn=MagicMock(return_value={"url": "URL", "file_token": "NEW"}),
            delete_fn=MagicMock(return_value=False),
        )

    assert result.status == "reply_sent_cleanup_pending"
    assert graph.aupdate_state.await_args_list[0].args[1] == {
        "pdf_token": "NEW",
        "attachment_tokens": ["KEEP", "CONCURRENT", "OLD"],
    }
    assert graph.aupdate_state.await_count == 1


@pytest.mark.asyncio
async def test_pdf_cleanup_removal_merges_handles_added_after_remote_delete(
    monkeypatch,
):
    state, dependencies, _store = _pdf_boundary(monkeypatch, old_token="OLD")
    state.values["attachment_tokens"] = ["KEEP"]
    latest_values = deepcopy(state.values)
    staged_values = deepcopy(latest_values)
    staged_values["pdf_token"] = "NEW"
    staged_values["attachment_tokens"] = ["KEEP", "OLD"]
    concurrent_values = deepcopy(staged_values)
    concurrent_values["attachment_tokens"] = ["KEEP", "OLD", "CONCURRENT"]
    graph = SimpleNamespace(
        aupdate_state=AsyncMock(),
        aget_state=AsyncMock(
            side_effect=[
                SimpleNamespace(values=latest_values),
                SimpleNamespace(values=staged_values),
                SimpleNamespace(values=concurrent_values),
            ]
        ),
    )

    with patch.object(lark_pdf_flow, "render_email_html", return_value="<html/>"), patch.object(
        lark_pdf_flow, "convert_html_to_pdf", return_value=b"PDF"
    ):
        result = await lark_pdf_flow.process_pdf_generation_and_reply(
            "msg-1",
            state,
            "msg_x",
            graph=graph,
            dependencies=dependencies,
            lark_api_client=_successful_lark_client(),
            upload_fn=MagicMock(return_value={"url": "URL", "file_token": "NEW"}),
            delete_fn=MagicMock(return_value=True),
        )

    assert result.status == "reply_sent"
    assert graph.aupdate_state.await_args_list[-1].args[1] == {
        "attachment_tokens": ["KEEP", "CONCURRENT"]
    }


@pytest.mark.asyncio
async def test_pdf_flow_serializes_same_email_transitions(monkeypatch):
    state, dependencies, _store = _pdf_boundary(monkeypatch, old_token="OLD")

    class StatefulGraph:
        def __init__(self):
            self.values = deepcopy(state.values)
            self.active_updates = 0
            self.max_active_updates = 0

        async def aget_state(self, _config):
            await asyncio.sleep(0)
            return SimpleNamespace(values=deepcopy(self.values))

        async def aupdate_state(self, _config, delta):
            self.active_updates += 1
            self.max_active_updates = max(
                self.max_active_updates,
                self.active_updates,
            )
            await asyncio.sleep(0)
            self.values.update(deepcopy(delta))
            self.active_updates -= 1

    graph = StatefulGraph()
    first_state = SimpleNamespace(values=deepcopy(state.values))
    second_state = SimpleNamespace(values=deepcopy(state.values))

    with patch.object(lark_pdf_flow, "render_email_html", return_value="<html/>"), patch.object(
        lark_pdf_flow, "convert_html_to_pdf", return_value=b"PDF"
    ):
        first, second = await asyncio.gather(
            lark_pdf_flow.process_pdf_generation_and_reply(
                "msg-1",
                first_state,
                "msg_a",
                graph=graph,
                dependencies=dependencies,
                lark_api_client=_successful_lark_client(),
                upload_fn=MagicMock(
                    return_value={"url": "URL-A", "file_token": "A"}
                ),
                delete_fn=MagicMock(return_value=False),
            ),
            lark_pdf_flow.process_pdf_generation_and_reply(
                "msg-1",
                second_state,
                "msg_b",
                graph=graph,
                dependencies=dependencies,
                lark_api_client=_successful_lark_client(),
                upload_fn=MagicMock(
                    return_value={"url": "URL-B", "file_token": "B"}
                ),
                delete_fn=MagicMock(return_value=False),
            ),
        )

    assert first.status == "reply_sent_cleanup_pending"
    assert second.status == "reply_sent_cleanup_pending"
    assert graph.max_active_updates == 1
    assert graph.values["pdf_token"] == "B"
    assert graph.values["attachment_tokens"] == ["OLD", "A"]


@pytest.mark.asyncio
async def test_pdf_replacement_reports_cleanup_untracked_when_handle_list_is_full(
    monkeypatch,
):
    state, dependencies, _store = _pdf_boundary(monkeypatch, old_token="OLD")
    state.values["attachment_tokens"] = [f"KEEP-{index}" for index in range(32)]
    graph = SimpleNamespace(aupdate_state=AsyncMock())

    with patch.object(lark_pdf_flow, "render_email_html", return_value="<html/>"), patch.object(
        lark_pdf_flow, "convert_html_to_pdf", return_value=b"PDF"
    ):
        result = await lark_pdf_flow.process_pdf_generation_and_reply(
            "msg-1",
            state,
            "msg_x",
            graph=graph,
            dependencies=dependencies,
            lark_api_client=_successful_lark_client(),
            upload_fn=MagicMock(return_value={"url": "URL", "file_token": "NEW"}),
            delete_fn=MagicMock(return_value=False),
        )

    assert result.status == "state_precondition_failed_cleanup_untracked"
    assert result.cleanup_tokens == ("NEW",)
    assert result.protected_tokens == ("OLD",)
    assert result.reply_sent is False
    graph.aupdate_state.assert_not_awaited()


@pytest.mark.asyncio
async def test_pdf_replacement_reports_cleanup_untracked_when_retention_write_fails(
    monkeypatch,
):
    state, dependencies, _store = _pdf_boundary(monkeypatch, old_token="OLD")
    state.values["attachment_tokens"] = ["KEEP"]
    graph = SimpleNamespace(
        aupdate_state=AsyncMock(
            side_effect=[None, RuntimeError("retain failed")]
        )
    )

    with patch.object(lark_pdf_flow, "render_email_html", return_value="<html/>"), patch.object(
        lark_pdf_flow, "convert_html_to_pdf", return_value=b"PDF"
    ):
        result = await lark_pdf_flow.process_pdf_generation_and_reply(
            "msg-1",
            state,
            "msg_x",
            graph=graph,
            dependencies=dependencies,
            lark_api_client=_successful_lark_client(),
            upload_fn=MagicMock(return_value={"url": "URL", "file_token": "NEW"}),
            delete_fn=MagicMock(return_value=True),
        )

    assert result.status == "reply_sent_cleanup_stale"
    assert result.cleanup_tokens == ("OLD",)
    assert result.reply_sent is True


@pytest.mark.asyncio
async def test_pdf_replacement_does_not_delete_old_after_concurrent_restore(
    monkeypatch,
):
    state, dependencies, _store = _pdf_boundary(monkeypatch, old_token="OLD")
    staged_values = deepcopy(state.values)
    staged_values["pdf_token"] = "OLD"
    staged_values["attachment_tokens"] = ["OLD"]
    graph = SimpleNamespace(
        aupdate_state=AsyncMock(),
        aget_state=AsyncMock(return_value=SimpleNamespace(values=staged_values)),
    )
    delete = MagicMock(return_value=True)

    with patch.object(lark_pdf_flow, "render_email_html", return_value="<html/>"), patch.object(
        lark_pdf_flow, "convert_html_to_pdf", return_value=b"PDF"
    ):
        result = await lark_pdf_flow.process_pdf_generation_and_reply(
            "msg-1",
            state,
            "msg_x",
            graph=graph,
            dependencies=dependencies,
            lark_api_client=_successful_lark_client(),
            upload_fn=MagicMock(return_value={"url": "URL", "file_token": "NEW"}),
            delete_fn=delete,
        )

    assert result.status == "reply_sent_cleanup_protected"
    assert result.protected_tokens == ("OLD", "NEW")
    delete.assert_not_called()


@pytest.mark.asyncio
async def test_pdf_replacement_reports_stale_registration_when_removal_write_fails(
    monkeypatch,
):
    state, dependencies, _store = _pdf_boundary(monkeypatch, old_token="OLD")
    graph = SimpleNamespace(
        aupdate_state=AsyncMock(
            side_effect=[None, RuntimeError("cleanup registration removal failed")]
        )
    )

    with patch.object(lark_pdf_flow, "render_email_html", return_value="<html/>"), patch.object(
        lark_pdf_flow, "convert_html_to_pdf", return_value=b"PDF"
    ):
        result = await lark_pdf_flow.process_pdf_generation_and_reply(
            "msg-1",
            state,
            "msg_x",
            graph=graph,
            dependencies=dependencies,
            lark_api_client=_successful_lark_client(),
            upload_fn=MagicMock(return_value={"url": "URL", "file_token": "NEW"}),
            delete_fn=MagicMock(return_value=True),
        )

    assert result.status == "reply_sent_cleanup_stale"
    assert result.cleanup_tokens == ("OLD",)
    assert result.reply_sent is True


@pytest.mark.asyncio
async def test_generate_returns_url_and_token_on_success(monkeypatch):
    state, dependencies, _store = _pdf_boundary(monkeypatch)
    upload = MagicMock(return_value={"url": "u", "file_token": "tok"})
    with patch.object(lark_pdf_flow, "render_email_html", return_value="<html/>"), \
         patch.object(lark_pdf_flow, "convert_html_to_pdf", return_value=b"PDF"):
        result = await lark_pdf_flow.generate_and_upload_pdf(
            "msg-1", state, dependencies=dependencies, upload_fn=upload
        )
    assert result == {"url": "u", "file_token": "tok"}
    upload.assert_called_once()
    args, _ = upload.call_args
    assert args[0] == "Email_Export_msg-1.pdf"
    assert args[1] == b"PDF"
    assert args[2] == 3


@pytest.mark.asyncio
async def test_generate_returns_none_when_pdf_empty(monkeypatch):
    state, dependencies, _store = _pdf_boundary(monkeypatch)
    with patch.object(lark_pdf_flow, "render_email_html", return_value="<html/>"), \
         patch.object(lark_pdf_flow, "convert_html_to_pdf", return_value=b""):
        result = await lark_pdf_flow.generate_and_upload_pdf(
            "msg-1", state, dependencies=dependencies, upload_fn=MagicMock()
        )
    assert result is None


@pytest.mark.asyncio
async def test_generate_returns_none_when_upload_fails(monkeypatch):
    state, dependencies, _store = _pdf_boundary(monkeypatch)
    upload = MagicMock(return_value=None)
    with patch.object(lark_pdf_flow, "render_email_html", return_value="<html/>"), \
         patch.object(lark_pdf_flow, "convert_html_to_pdf", return_value=b"PDF"):
        result = await lark_pdf_flow.generate_and_upload_pdf(
            "msg-1", state, dependencies=dependencies, upload_fn=upload
        )
    assert result is None


@pytest.mark.asyncio
async def test_generate_returns_none_when_pdf_conversion_raises(monkeypatch):
    state, dependencies, _store = _pdf_boundary(monkeypatch)
    def explode(*_a, **_k):
        raise RuntimeError("bad pdf")
    with patch.object(lark_pdf_flow, "render_email_html", return_value="<html/>"), \
         patch.object(lark_pdf_flow, "convert_html_to_pdf", side_effect=explode):
        result = await lark_pdf_flow.generate_and_upload_pdf(
            "msg-1", state, dependencies=dependencies, upload_fn=MagicMock()
        )
    assert result is None


@pytest.mark.asyncio
async def test_process_pdf_skips_reply_when_pdf_generation_fails(monkeypatch):
    fake_state, dependencies, _store = _pdf_boundary(monkeypatch)
    fake_graph = SimpleNamespace(aupdate_state=AsyncMock())
    fake_lark = MagicMock()
    upload = MagicMock(return_value=None)

    with patch.object(lark_pdf_flow, "render_email_html", return_value="<html/>"), \
         patch.object(lark_pdf_flow, "convert_html_to_pdf", return_value=b"PDF"):
        await lark_pdf_flow.process_pdf_generation_and_reply(
            "msg-1",
            fake_state,
            "msg_x",
            graph=fake_graph,
            dependencies=dependencies,
            lark_api_client=fake_lark,
            upload_fn=upload,
            delete_fn=MagicMock(),
        )

    fake_lark.im.v1.message.reply.assert_not_called()
    fake_graph.aupdate_state.assert_not_awaited()


@pytest.mark.asyncio
async def test_process_pdf_persists_token_and_replies_on_success(monkeypatch):
    fake_state, dependencies, _store = _pdf_boundary(monkeypatch)
    fake_graph = SimpleNamespace(aupdate_state=AsyncMock())
    fake_lark = MagicMock()
    upload = MagicMock(return_value={"url": "URL", "file_token": "TOK"})

    with patch.object(lark_pdf_flow, "render_email_html", return_value="<html/>"), \
         patch.object(lark_pdf_flow, "convert_html_to_pdf", return_value=b"PDF"):
        await lark_pdf_flow.process_pdf_generation_and_reply(
            "msg-1",
            fake_state,
            "msg_x",
            graph=fake_graph,
            dependencies=dependencies,
            lark_api_client=fake_lark,
            upload_fn=upload,
            delete_fn=MagicMock(),
        )

    fake_graph.aupdate_state.assert_awaited_once()
    fake_lark.im.v1.message.reply.assert_called_once()


@pytest.mark.asyncio
async def test_process_pdf_skips_reply_when_lark_client_missing(monkeypatch):
    fake_state, dependencies, _store = _pdf_boundary(monkeypatch)
    fake_graph = SimpleNamespace(aupdate_state=AsyncMock())
    upload = MagicMock(return_value={"url": "URL", "file_token": "TOK"})

    with patch.object(lark_pdf_flow, "render_email_html", return_value="<html/>"), \
         patch.object(lark_pdf_flow, "convert_html_to_pdf", return_value=b"PDF"):
        await lark_pdf_flow.process_pdf_generation_and_reply(
            "msg-1",
            fake_state,
            "msg_x",
            graph=fake_graph,
            dependencies=dependencies,
            lark_api_client=None,
            upload_fn=upload,
            delete_fn=MagicMock(),
        )

    # No client means no remote file is created and no token enters Graph.
    fake_graph.aupdate_state.assert_not_awaited()
    upload.assert_not_called()
