from __future__ import annotations

import json
import hashlib
import hmac
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.utils import lark_app


def _settings(*, allowed: object, debug: bool = False) -> SimpleNamespace:
    return SimpleNamespace(
        LARK_ALLOWED_OPEN_IDS=allowed,
        DEBUG=debug,
        EXTERNAL_URL="https://preview.example.invalid",
    )


def _is_allowed(actor: object, configured: object) -> bool:
    from src.security.auth import is_lark_operator_allowed

    return is_lark_operator_allowed(actor, _settings(allowed=configured))


def _card_event(
    action: str,
    *,
    actor: object = "ou_denied",
    email_id: object = "mail-authorization-test",
    message_id: object = "om-authorization-test",
) -> SimpleNamespace:
    return SimpleNamespace(
        event=SimpleNamespace(
            action=SimpleNamespace(
                value={"action": action, "id": email_id},
                option=None,
                options=None,
                form_value={},
            ),
            operator=SimpleNamespace(open_id=actor),
            context=SimpleNamespace(open_message_id=message_id),
        )
    )


def _p2p_event(
    text: str,
    *,
    actor: object = "ou_denied",
) -> SimpleNamespace:
    return SimpleNamespace(
        event=SimpleNamespace(
            sender=SimpleNamespace(
                sender_id=SimpleNamespace(open_id=actor),
            ),
            message=SimpleNamespace(
                message_type="text",
                chat_type="p2p",
                content=json.dumps({"text": text}),
            ),
        )
    )


def _toast_type(response: object) -> object:
    if isinstance(response, dict):
        toast = response.get("toast") or {}
        if isinstance(toast, dict):
            return toast.get("type")
        return getattr(toast, "type", None)
    return getattr(getattr(response, "toast", None), "type", None)


def test_lark_allowlist_trims_configured_entries_and_matches_exactly():
    configured = " ou_allowed,ou_second , ou_allowed "

    assert _is_allowed("ou_allowed", configured) is True
    assert _is_allowed("ou_second", configured) is True
    assert _is_allowed("OU_ALLOWED", configured) is False
    assert _is_allowed("ou_allow", configured) is False
    assert _is_allowed(" ou_allowed ", configured) is False


def test_lark_allowlist_compares_every_candidate_with_constant_time_primitive():
    real_compare = hmac.compare_digest

    with pytest.MonkeyPatch.context() as monkeypatch:
        compare = MagicMock(wraps=real_compare)
        monkeypatch.setattr("src.security.auth.hmac.compare_digest", compare)
        allowed = _is_allowed("ou_allowed", "ou_first,ou_allowed,ou_last")

    assert allowed is True
    assert [item.args for item in compare.call_args_list] == [
        ("ou_allowed", "ou_first"),
        ("ou_allowed", "ou_allowed"),
        ("ou_allowed", "ou_last"),
    ]


@pytest.mark.parametrize(
    ("configured", "actor"),
    [
        ("", "ou_allowed"),
        ("   ", "ou_allowed"),
        (", ,", "ou_allowed"),
        (None, "ou_allowed"),
        ("*", "ou_allowed"),
        ("ou_allowed", ""),
        ("ou_allowed", "   "),
        ("ou_allowed", None),
        ("ou_allowed", 123),
    ],
)
def test_lark_allowlist_empty_or_invalid_values_deny(
    configured: object,
    actor: object,
):
    assert _is_allowed(actor, configured) is False


@pytest.mark.parametrize(
    "action",
    ["approve", "view_original", "mark_read", "save_draft_only"],
)
def test_unlisted_card_operator_is_rejected_before_state_or_action(
    monkeypatch,
    action: str,
):
    graph = SimpleNamespace(
        aget_state=MagicMock(
            side_effect=AssertionError("unauthorized_graph_lookup")
        ),
        aupdate_state=MagicMock(
            side_effect=AssertionError("unauthorized_graph_update")
        ),
        ainvoke=MagicMock(
            side_effect=AssertionError("unauthorized_graph_resume")
        ),
    )
    database = MagicMock()
    approval = MagicMock(
        side_effect=AssertionError("unauthorized_approval_action")
    )
    draft_claim = MagicMock(
        side_effect=AssertionError("unauthorized_draft_action")
    )
    reply = MagicMock(
        side_effect=AssertionError("unauthorized_lark_reply")
    )

    monkeypatch.setattr(
        lark_app,
        "get_settings",
        lambda: _settings(allowed="ou_allowed"),
    )
    monkeypatch.setattr(lark_app, "graph", graph)
    monkeypatch.setattr(lark_app, "db_manager", database)
    monkeypatch.setattr(lark_app, "process_approval", approval)
    monkeypatch.setattr(lark_app, "_claim_draft_save_action", draft_claim)
    monkeypatch.setattr(
        lark_app,
        "lark_api_client",
        SimpleNamespace(
            im=SimpleNamespace(
                v1=SimpleNamespace(
                    message=SimpleNamespace(reply=reply),
                )
            )
        ),
    )

    response = lark_app.handle_card_action(_card_event(action))

    assert _toast_type(response) in {"error", "warning"}
    graph.aget_state.assert_not_called()
    graph.aupdate_state.assert_not_called()
    graph.ainvoke.assert_not_called()
    assert database.method_calls == []
    approval.assert_not_called()
    draft_claim.assert_not_called()
    reply.assert_not_called()


def test_unlisted_p2p_sender_is_rejected_before_dispatch_or_reply(
    monkeypatch,
):
    router = SimpleNamespace(
        dispatch=AsyncMock(return_value="PRIVATE-COMMAND-RESULT")
    )
    create_message = MagicMock()
    monkeypatch.setattr(
        lark_app,
        "get_settings",
        lambda: _settings(allowed="ou_allowed"),
    )
    monkeypatch.setattr(lark_app, "_command_router", router)
    monkeypatch.setattr(
        lark_app,
        "lark_api_client",
        SimpleNamespace(
            im=SimpleNamespace(
                v1=SimpleNamespace(
                    message=SimpleNamespace(create=create_message),
                )
            )
        ),
    )

    lark_app._handle_p2_im_message_receive(
        _p2p_event("/pending", actor="ou_denied")
    )

    router.dispatch.assert_not_awaited()
    create_message.assert_not_called()


def test_allowed_card_operator_reaches_action_once(monkeypatch):
    state = SimpleNamespace(
        values={
            "email": {"subject": "bounded subject"},
        }
    )
    graph = SimpleNamespace(aget_state=AsyncMock(return_value=state))
    approval = MagicMock(return_value=False)

    def close_and_return(coroutine):
        coroutine.close()
        return state

    monkeypatch.setattr(
        lark_app,
        "get_settings",
        lambda: _settings(allowed="ou_allowed"),
    )
    monkeypatch.setattr(lark_app, "graph", graph)
    monkeypatch.setattr(lark_app, "safe_async_wait", close_and_return)
    monkeypatch.setattr(lark_app, "process_approval", approval)

    response = lark_app.handle_card_action(
        _card_event("approve", actor="ou_allowed")
    )

    assert _toast_type(response) == "warning"
    assert graph.aget_state.call_count == 1
    approval.assert_called_once_with(
        "mail-authorization-test",
        "ou_allowed",
    )


@pytest.mark.parametrize("missing", ["message_id", "email_id"])
def test_card_missing_required_identifier_is_rejected_before_state_lookup(
    monkeypatch,
    missing: str,
):
    graph = SimpleNamespace(
        aget_state=MagicMock(
            side_effect=AssertionError("invalid_event_graph_lookup")
        )
    )
    monkeypatch.setattr(
        lark_app,
        "get_settings",
        lambda: _settings(allowed="ou_allowed"),
    )
    monkeypatch.setattr(lark_app, "graph", graph)
    event = _card_event(
        "approve",
        actor="ou_allowed",
        email_id=None if missing == "email_id" else "mail-valid",
        message_id=None if missing == "message_id" else "om-valid",
    )

    response = lark_app.handle_card_action(event)

    assert _toast_type(response) in {"error", "warning"}
    graph.aget_state.assert_not_called()


def test_unauthorized_card_log_omits_raw_identifiers(monkeypatch, caplog):
    raw_actor = "ou_RAW-ACTOR-SENTINEL"
    raw_email_id = "RAW-EMAIL-ID-SENTINEL"
    raw_message_id = "om_RAW-MESSAGE-SENTINEL"
    graph = SimpleNamespace(
        aget_state=MagicMock(
            side_effect=AssertionError("unauthorized_graph_lookup")
        )
    )
    monkeypatch.setattr(
        lark_app,
        "get_settings",
        lambda: _settings(allowed="ou_allowed"),
    )
    monkeypatch.setattr(lark_app, "graph", graph)
    caplog.set_level(logging.INFO, logger=lark_app.__name__)

    response = lark_app.handle_card_action(
        _card_event(
            "approve",
            actor=raw_actor,
            email_id=raw_email_id,
            message_id=raw_message_id,
        )
    )

    assert _toast_type(response) in {"error", "warning"}
    assert raw_actor not in caplog.text
    assert raw_email_id not in caplog.text
    assert raw_message_id not in caplog.text
    graph.aget_state.assert_not_called()


def test_production_view_original_is_disabled_before_content_hydration(monkeypatch):
    state = SimpleNamespace(values={"email": {"subject": "bounded"}})
    graph = SimpleNamespace(aget_state=AsyncMock(return_value=state))

    def return_state(coroutine):
        coroutine.close()
        return state

    monkeypatch.setattr(
        lark_app,
        "get_settings",
        lambda: _settings(allowed="ou_allowed", debug=False),
    )
    monkeypatch.setattr(lark_app, "graph", graph)
    monkeypatch.setattr(lark_app, "safe_async_wait", return_state)
    monkeypatch.setattr(
        lark_app,
        "_hydrate_lark_projection",
        MagicMock(side_effect=AssertionError("preview_content_hydrated")),
    )

    response = lark_app.handle_card_action(
        _card_event("view_original", actor="ou_allowed")
    )

    assert response == {
        "toast": {
            "type": "warning",
            "content": "Web 原文预览暂未开放，请使用 PDF",
        }
    }


def test_lark_signature_verification_fails_closed_without_encrypt_key(monkeypatch):
    monkeypatch.setattr(
        lark_app,
        "get_settings",
        lambda: SimpleNamespace(LARK_ENCRYPT_KEY=""),
    )

    assert lark_app.verify_lark_signature("1", "n", "{}", "anything") is False


def test_lark_signature_verification_accepts_only_exact_digest(monkeypatch):
    key = "lark-encrypt-key"
    timestamp = "123"
    nonce = "nonce"
    body = '{"event":"safe"}'
    expected = hashlib.sha256(
        f"{timestamp}{nonce}{key}{body}".encode("utf-8")
    ).hexdigest()
    monkeypatch.setattr(
        lark_app,
        "get_settings",
        lambda: SimpleNamespace(LARK_ENCRYPT_KEY=key),
    )

    assert lark_app.verify_lark_signature(timestamp, nonce, body, expected) is True
    assert lark_app.verify_lark_signature(timestamp, nonce, body, "wrong") is False
