from __future__ import annotations

import sys
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from scripts import (
    manual_nodes_test,
    push_test_card,
    reprocess_email,
    test_notification_logic,
)
from src.graph.dependencies import GraphDependencies
from src.graph.state_factory import content_ref_from_json
from src.domain.email_state import ProcessingOutcome
from src.utils import lark_app


@pytest.mark.asyncio
async def test_reprocess_script_passes_shared_graph_dependencies(monkeypatch):
    dependencies = MagicMock(name="graph_dependencies")
    ctx = SimpleNamespace(
        db_manager=MagicMock(name="db_manager"),
        graph=MagicMock(name="graph"),
        exchange_client=MagicMock(name="exchange_client"),
        graph_dependencies=dependencies,
        setup_async=AsyncMock(),
        close=AsyncMock(),
    )
    init_lark = MagicMock()

    monkeypatch.setattr("src.init_app.get_app_context", lambda: ctx)
    monkeypatch.setattr(lark_app, "init_lark_app", init_lark)
    monkeypatch.setattr(reprocess_email, "list_stuck_emails", AsyncMock(return_value=[]))
    monkeypatch.setattr(sys, "argv", ["reprocess_email.py", "--list-stuck"])

    await reprocess_email.main()

    ctx.setup_async.assert_awaited_once_with()
    init_lark.assert_called_once()
    assert init_lark.call_args.kwargs["dependencies"] is dependencies
    ctx.close.assert_awaited_once_with()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("outcome", "final_status", "expected"),
    [
        (ProcessingOutcome.PROCESSED, "waiting_approval", True),
        (ProcessingOutcome.PROCESSED, "error", False),
        (ProcessingOutcome.PROCESSED, "delivery_failed", False),
        (ProcessingOutcome.DUPLICATE, None, False),
    ],
)
async def test_reprocess_single_preserves_row_and_uses_force_contract(
    monkeypatch,
    outcome,
    final_status,
    expected,
):
    get_email_status = AsyncMock(return_value=final_status)
    ctx = SimpleNamespace(
        exchange_client=SimpleNamespace(
            get_email=AsyncMock(
                return_value={
                    "id": "mail-1",
                    "subject": "subject",
                    "attachments": [],
                }
            )
        ),
        db_manager=SimpleNamespace(
            get_connection=MagicMock(),
            get_email_status=get_email_status,
        ),
    )
    process = AsyncMock(return_value=outcome)
    monkeypatch.setattr(
        "src.exchange_service.process_and_archive_email",
        process,
    )

    assert await reprocess_email.reprocess_single("mail-1", ctx) is expected
    ctx.db_manager.get_connection.assert_not_called()
    if outcome is ProcessingOutcome.PROCESSED:
        get_email_status.assert_awaited_once_with("mail-1")
    else:
        get_email_status.assert_not_awaited()
    process.assert_awaited_once_with(
        {
            "id": "mail-1",
            "subject": "subject",
            "attachments": [],
        },
        ctx,
        force_reprocess=True,
    )


@pytest.mark.asyncio
async def test_manual_node_script_uses_dependencies_and_slim_state(mock_env):
    email = {
        "id": "manual-node-smoke",
        "subject": "Test Email",
        "sender": "test@example.com",
        "body": "MANUAL-BODY-SENTINEL",
        "attachments": [
            {
                "name": "image.png",
                "content": "MANUAL-BASE64-SENTINEL",
            }
        ],
    }
    observed_states = []
    observed_dependencies = []

    async def fake_categorizer(state, dependencies):
        observed_states.append(deepcopy(state))
        observed_dependencies.append(dependencies)
        hydrated = await dependencies.content_store.load_email(
            content_ref_from_json(state["content_ref"]),
        )
        assert hydrated["body"] == "MANUAL-BODY-SENTINEL"
        return {
            "classification": {"need_reply": True, "summary": "test"},
            "next_step": "drafter",
        }

    async def fake_drafter(state, dependencies):
        observed_states.append(deepcopy(state))
        observed_dependencies.append(dependencies)
        draft_id = await dependencies.drafts.save_draft(
            state["email_id"],
            "MANUAL-DRAFT-SENTINEL",
        )
        return {"draft_id": draft_id, "next_step": "approval"}

    final_state, draft = await manual_nodes_test.run_node_smoke(
        email_data=email,
        categorize_fn=fake_categorizer,
        draft_fn=fake_drafter,
    )

    assert len(observed_dependencies) == 2
    assert all(isinstance(item, GraphDependencies) for item in observed_dependencies)
    for state in observed_states:
        assert "body" not in state["email"]
        assert "attachments" not in state["email"]
        assert "draft" not in state
        assert "MANUAL-BODY-SENTINEL" not in str(state)
        assert "MANUAL-BASE64-SENTINEL" not in str(state)
        assert "MANUAL-DRAFT-SENTINEL" not in str(state)
    assert final_state["draft_id"] == "manual-node-smoke"
    assert draft == "MANUAL-DRAFT-SENTINEL"


@pytest.mark.asyncio
async def test_push_test_pdf_uses_explicit_test_boundary_not_graph(mock_env, monkeypatch):
    email = {
        "id": "test_push_script_smoke",
        "subject": "Test PDF",
        "sender": "sender@example.com",
        "to": ["recipient@example.com"],
        "cc": [],
        "body": "TEST-PUSH-BODY-SENTINEL",
        "attachments": [
            {
                "name": "image.png",
                "content": "TEST-PUSH-BASE64-SENTINEL",
            }
        ],
    }
    upload_fn = MagicMock(name="upload_fn")
    delete_fn = MagicMock(name="delete_fn")
    expected = {"url": "https://example.test/pdf", "file_token": "pdf-token"}
    generate_pdf = AsyncMock(return_value=expected)
    monkeypatch.setattr(
        push_test_card.lark_pdf_flow,
        "generate_and_upload_pdf",
        generate_pdf,
    )

    result = await push_test_card.generate_test_card_pdf(
        email,
        draft="TEST-PUSH-DRAFT-SENTINEL",
        upload_fn=upload_fn,
        delete_fn=delete_fn,
    )

    assert result == expected
    generate_pdf.assert_awaited_once()
    email_id, state = generate_pdf.await_args.args
    dependencies = generate_pdf.await_args.kwargs["dependencies"]
    assert email_id == email["id"]
    assert isinstance(dependencies, GraphDependencies)
    assert generate_pdf.await_args.kwargs["upload_fn"] is upload_fn
    assert generate_pdf.await_args.kwargs["delete_fn"] is delete_fn
    assert "body" not in state.values["email"]
    assert "attachments" not in state.values["email"]
    assert "draft" not in state.values
    assert "TEST-PUSH-BODY-SENTINEL" not in str(state.values)
    assert "TEST-PUSH-BASE64-SENTINEL" not in str(state.values)
    assert "TEST-PUSH-DRAFT-SENTINEL" not in str(state.values)

    ref = content_ref_from_json(state.values["content_ref"])
    hydrated = await dependencies.content_store.load_email(
        ref,
        include_attachments=True,
    )
    assert hydrated == email
    assert await dependencies.drafts.load_draft(state.values["draft_id"]) == (
        "TEST-PUSH-DRAFT-SENTINEL"
    )


def test_push_test_state_registration_is_explicitly_in_memory(monkeypatch):
    email_id = "test_push_script_registration"
    email = {
        "id": email_id,
        "subject": "Test",
        "sender": "sender@example.com",
        "to": [],
        "cc": [],
        "body": "TEST-CARD-BODY",
    }
    mock_store = {}
    monkeypatch.setattr(lark_app, "_mock_store", mock_store)

    push_test_card.register_test_card_state(
        email,
        draft="TEST-CARD-DRAFT",
        context=[{"chunk_text": "context"}],
        classification={"reasoning": "test"},
        attachment_tokens=["attachment-token"],
        pdf_token="pdf-token",
    )

    values = mock_store[email_id].values
    assert values["email"] == email
    assert values["draft"] == "TEST-CARD-DRAFT"
    assert values["attachment_tokens"] == ["attachment-token"]
    assert values["pdf_token"] == "pdf-token"


@pytest.mark.asyncio
async def test_notification_script_uses_current_pdf_and_dispatch_contracts():
    results = await test_notification_logic.run_notification_smoke()

    assert results == [
        {"delivered": True, "kind": "read_only"},
        {"delivered": True, "kind": "read_only"},
        {"delivered": True, "kind": "skipped"},
    ]


@pytest.mark.asyncio
async def test_push_test_main_sends_pdf_url_string(mock_env, monkeypatch):
    email_data = {
        "id": "test_push_REAL_EML",
        "subject": "Test",
        "sender": "sender@example.com",
        "to": [],
        "cc": [],
        "received_at": "2026-07-11",
        "body": "body",
        "attachments": [],
    }
    message = MagicMock()
    message.__getitem__.side_effect = {
        "subject": email_data["subject"],
        "from": email_data["sender"],
        "to": "",
        "cc": "",
        "date": email_data["received_at"],
    }.__getitem__
    message.walk.return_value = []
    lark_client = MagicMock()
    send_card = MagicMock(return_value=True)

    monkeypatch.setenv("LARK_APP_ID", "app-id")
    monkeypatch.setenv("LARK_CHAT_ID", "chat-id")
    monkeypatch.setattr(push_test_card.os.path, "exists", lambda _path: True)
    monkeypatch.setattr("email.message_from_binary_file", lambda *_args, **_kwargs: message)
    monkeypatch.setattr("builtins.open", MagicMock())
    monkeypatch.setattr(push_test_card, "_extract_body", lambda _message: "body")
    inject_debug = AsyncMock()
    monkeypatch.setattr(push_test_card, "_inject_debug_original", inject_debug)
    monkeypatch.setattr(
        push_test_card,
        "generate_test_card_pdf",
        AsyncMock(
            return_value={
                "url": "https://example.test/pdf",
                "file_token": "pdf-token",
            }
        ),
    )
    monkeypatch.setattr(push_test_card, "register_test_card_state", MagicMock())
    monkeypatch.setattr(lark_app, "init_lark_app", MagicMock())
    monkeypatch.setattr(lark_app, "lark_api_client", lark_client)
    monkeypatch.setattr(lark_app, "send_approval_card", send_card)

    await push_test_card.main()

    assert send_card.call_args.kwargs["pdf_url"] == "https://example.test/pdf"
    assert not isinstance(send_card.call_args.kwargs["pdf_url"], dict)
    inject_debug.assert_awaited_once_with(
        email_data,
        draft="Thank you, I have received the update.",
        context=[{"chunk_text": "Flight details preview..."}],
        classification={"reasoning": "This is a test notification sent manually."},
        attachment_tokens=[],
        pdf_token="pdf-token",
    )


def _configure_push_test_main_pdf_outcome(monkeypatch, outcome):
    message = MagicMock()
    message.__getitem__.side_effect = {
        "subject": "Test",
        "from": "sender@example.com",
        "to": "",
        "cc": "",
        "date": "2026-07-11",
    }.__getitem__
    message.walk.return_value = []
    send_card = MagicMock(return_value=True)
    register_state = MagicMock()
    inject_debug = AsyncMock()
    delete_file = MagicMock(return_value=False)

    monkeypatch.setenv("LARK_APP_ID", "app-id")
    monkeypatch.setenv("LARK_CHAT_ID", "chat-id")
    monkeypatch.setattr(push_test_card.os.path, "exists", lambda _path: True)
    monkeypatch.setattr("email.message_from_binary_file", lambda *_args, **_kwargs: message)
    monkeypatch.setattr("builtins.open", MagicMock())
    monkeypatch.setattr(push_test_card, "_extract_body", lambda _message: "body")
    monkeypatch.setattr(push_test_card, "_inject_debug_original", inject_debug)
    monkeypatch.setattr(
        push_test_card,
        "generate_test_card_pdf",
        AsyncMock(return_value=outcome),
    )
    monkeypatch.setattr(push_test_card, "register_test_card_state", register_state)
    monkeypatch.setattr(lark_app, "init_lark_app", MagicMock())
    monkeypatch.setattr(lark_app, "lark_api_client", MagicMock())
    monkeypatch.setattr(lark_app, "send_approval_card", send_card)
    monkeypatch.setattr(lark_app, "delete_file_from_drive", delete_file)
    return send_card, register_state, inject_debug, delete_file


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("outcome", "expected_delete_count"),
    [
        (
            push_test_card.lark_pdf_flow.PdfFlowOutcome(
                status="upload_invalid_cleanup_required",
                retryable=True,
                cleanup_tokens=("ORPHAN",),
            ),
            1,
        ),
        (
            push_test_card.lark_pdf_flow.PdfFlowOutcome(
                status="upload_invalid_protected_token",
                retryable=True,
                protected_tokens=("PROTECTED",),
            ),
            0,
        ),
    ],
)
async def test_push_test_main_fails_closed_on_unresolved_pdf_outcome(
    mock_env,
    monkeypatch,
    outcome,
    expected_delete_count,
):
    send_card, register_state, inject_debug, delete_file = (
        _configure_push_test_main_pdf_outcome(monkeypatch, outcome)
    )

    with pytest.raises(RuntimeError, match="test_card_pdf_unresolved") as exc_info:
        await push_test_card.main()

    assert getattr(exc_info.value, "cleanup_tokens", ()) == outcome.cleanup_tokens
    assert getattr(exc_info.value, "protected_tokens", ()) == outcome.protected_tokens
    assert delete_file.call_count == expected_delete_count
    send_card.assert_not_called()
    register_state.assert_not_called()
    inject_debug.assert_not_awaited()


@pytest.mark.asyncio
async def test_push_test_main_continues_without_pdf_after_cleanup_recovery(
    mock_env,
    monkeypatch,
):
    outcome = push_test_card.lark_pdf_flow.PdfFlowOutcome(
        status="upload_invalid_cleanup_required",
        retryable=True,
        cleanup_tokens=("ORPHAN",),
    )
    send_card, register_state, inject_debug, delete_file = (
        _configure_push_test_main_pdf_outcome(monkeypatch, outcome)
    )
    delete_file.return_value = True

    await push_test_card.main()

    delete_file.assert_called_once_with("ORPHAN")
    assert send_card.call_args.kwargs["pdf_url"] is None
    assert register_state.call_args.kwargs["pdf_token"] is None
    assert inject_debug.await_args.kwargs["pdf_token"] is None
