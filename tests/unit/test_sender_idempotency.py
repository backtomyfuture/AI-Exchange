import asyncio
import json
import logging
import time
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.graph.dependencies import GraphDependencies
from src.graph.state_factory import build_initial_graph_state
from src.domain.errors import DatabaseOperationError
from src.domain.send_result import ExchangeSendResult
from src.nodes.sender import send_final_email
from src.storage import ContentRef


BODY_SENTINEL = "SENDER-BODY-SECRET-SENTINEL"
DRAFT_SENTINEL = "SENDER-DRAFT-SECRET-SENTINEL"
EXCEPTION_SENTINEL = "SENDER-REMOTE-EXCEPTION-SENTINEL"


@pytest.fixture(autouse=True)
def _configured_content_account(monkeypatch):
    monkeypatch.setattr(
        "src.graph.state_factory.get_settings",
        lambda: SimpleNamespace(EXCHANGE_ACCOUNT_ID=8),
    )


class FakeContentStore:
    def __init__(self, email):
        self.email = deepcopy(email)
        self.loads = []

    async def load_email(self, ref, *, include_attachments=False):
        self.loads.append((ref, include_attachments))
        return deepcopy(self.email)


class FakeDraftStore:
    def __init__(self, draft):
        self.draft = draft
        self.loads = []

    async def save_draft(self, email_id, content):
        self.draft = content
        return email_id

    async def load_draft(self, draft_id):
        self.loads.append(draft_id)
        return self.draft


class LockBackedFakeDB:
    """Model the database row as the cross-coroutine send-winner boundary."""

    def __init__(self, status="approved", *, fail_targets=()):
        self.status = status
        self.error_message = None
        self.fail_targets = set(fail_targets)
        self.transitions = []
        self.legacy_updates = []
        self._lock = asyncio.Lock()

    async def compare_and_set_status(self, email_id, *, expected, target):
        async with self._lock:
            before = self.status
            won = before in expected and target not in self.fail_targets
            if won:
                self.status = target
            self.transitions.append(
                {
                    "email_id": email_id,
                    "expected": frozenset(expected),
                    "target": target,
                    "before": before,
                    "won": won,
                }
            )
            await asyncio.sleep(0)
            return won

    async def get_email_status(self, email_id):
        del email_id
        return self.status

    async def compare_and_set_manual_review(
        self,
        email_id,
        *,
        expected,
        error_code,
    ):
        async with self._lock:
            before = self.status
            won = before in expected and "manual_review" not in self.fail_targets
            if won:
                self.status = "manual_review"
                self.error_message = error_code
            self.transitions.append(
                {
                    "email_id": email_id,
                    "expected": frozenset(expected),
                    "target": "manual_review",
                    "before": before,
                    "won": won,
                }
            )
            return won

    async def compare_and_set_send_unknown(self, email_id, *, error_code):
        async with self._lock:
            before = self.status
            won = before == "sending" and "send_unknown" not in self.fail_targets
            if won:
                self.status = "send_unknown"
                self.error_message = error_code
            self.transitions.append(
                {
                    "email_id": email_id,
                    "expected": frozenset({"sending"}),
                    "target": "send_unknown",
                    "before": before,
                    "won": won,
                }
            )
            return won

    async def update_status(self, email_id, status, **kwargs):
        # Kept only so the pre-Task-8 implementation fails assertions instead
        # of crashing because an old non-CAS method is absent.
        self.legacy_updates.append((email_id, status, kwargs))
        if status is not None:
            self.status = status


def _ref():
    return ContentRef(
        account_id=8,
        object_id="00000000-0000-4000-8000-000000000008",
        key_version="v1",
        sha256="8" * 64,
    )


def _state(*, action="reply", draft_to=None, draft_cc=None):
    email = {
        "id": "mail-send-1",
        "subject": "bounded subject",
        "sender": "sender@example.com",
        "body": BODY_SENTINEL,
        "draft_to": ["recipient@example.com"],
        "draft_cc": [],
    }
    state = build_initial_graph_state(email, _ref())
    state.update(
        {
            "classification": {"action": action},
            "draft_id": email["id"],
            "draft_to": (
                ["recipient@example.com"] if draft_to is None else list(draft_to)
            ),
            "draft_cc": [] if draft_cc is None else list(draft_cc),
            "approval_status": "approved",
        }
    )
    return state


def _runtime(*, status="approved", remote_result=True, fail_targets=()):
    email = {
        "id": "mail-send-1",
        "subject": "bounded subject",
        "sender": "sender@example.com",
        "body": BODY_SENTINEL,
        "attachments": [{"name": "private", "content": "BASE64-SECRET"}],
    }
    dependencies = GraphDependencies(
        content_store=FakeContentStore(email),
        drafts=FakeDraftStore(DRAFT_SENTINEL),
    )
    if isinstance(remote_result, BaseException):
        reply = AsyncMock(side_effect=remote_result)
        forward = AsyncMock(side_effect=remote_result)
    else:
        typed_result = (
            ExchangeSendResult.sent()
            if remote_result is True
            else ExchangeSendResult.unknown()
        )
        reply = AsyncMock(return_value=typed_result)
        forward = AsyncMock(return_value=typed_result)
    db = LockBackedFakeDB(status, fail_targets=fail_targets)
    ctx = SimpleNamespace(
        exchange_client=SimpleNamespace(
            reply_email_result=reply,
            forward_email_result=forward,
            reply_email=reply,
            forward_email=forward,
        ),
        db_manager=db,
        email_processor=SimpleNamespace(process_sent_email=MagicMock()),
    )
    return dependencies, ctx, db


def _assert_manual_delta(result, *, code=None):
    assert result["next_step"] == "manual_review"
    assert result["approval_status"] == "manual_review"
    assert isinstance(result["safe_error_summary"], str)
    assert 0 < len(result["safe_error_summary"].encode("utf-8")) <= 256
    if code is not None:
        assert result["safe_error_summary"] == code


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["reply", "forward"])
async def test_concurrent_sender_invocations_make_one_remote_call(action):
    dependencies, ctx, db = _runtime()
    state = _state(action=action, draft_cc=["copy@example.com"])

    with patch("src.init_app.get_app_context", return_value=ctx):
        results = await asyncio.gather(
            send_final_email(deepcopy(state), dependencies),
            send_final_email(deepcopy(state), dependencies),
        )

    selected_remote = (
        ctx.exchange_client.forward_email_result
        if action == "forward"
        else ctx.exchange_client.reply_email_result
    )
    other_remote = (
        ctx.exchange_client.reply_email_result
        if action == "forward"
        else ctx.exchange_client.forward_email_result
    )
    assert selected_remote.await_count == 1
    other_remote.assert_not_awaited()
    assert db.status == "sent"
    assert sum(t["won"] and t["target"] == "sending" for t in db.transitions) == 1
    assert sum(t["won"] and t["target"] == "sent" for t in db.transitions) == 1
    assert all(result.get("next_step") == "end" for result in results)


@pytest.mark.asyncio
async def test_send_claim_loser_has_no_exchange_side_effect():
    dependencies, ctx, db = _runtime(status="sending")

    with patch("src.init_app.get_app_context", return_value=ctx):
        result = await send_final_email(_state(), dependencies)

    ctx.exchange_client.reply_email.assert_not_awaited()
    ctx.exchange_client.forward_email.assert_not_awaited()
    ctx.email_processor.process_sent_email.assert_not_called()
    assert db.status == "sending"
    assert result["next_step"] == "end"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("draft_to", "action"),
    [
        (["open_id=unresolvable-user"], "reply"),
        ([], "reply"),
        ([], "forward"),
    ],
)
async def test_unresolved_or_empty_required_recipient_moves_to_manual_without_send(
    draft_to,
    action,
):
    dependencies, ctx, db = _runtime()

    with patch("src.init_app.get_app_context", return_value=ctx), patch(
        "src.utils.lark_app.lark_api_client", None
    ):
        result = await send_final_email(
            _state(action=action, draft_to=draft_to),
            dependencies,
        )

    _assert_manual_delta(result)
    assert db.status == "manual_review"
    ctx.exchange_client.reply_email.assert_not_awaited()
    ctx.exchange_client.forward_email.assert_not_awaited()
    ctx.email_processor.process_sent_email.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("draft_to", "draft_cc"),
    [
        (["not-an-email"], []),
        (["bad@@example.com"], []),
        (["recipient@example.com"], ["invalid-copy"]),
        (["first@example.com,second@example.com"], []),
        (["victim@example.com garbage"], []),
        (["valid@example.com\x00"], []),
        (["a..b@example.com"], []),
        (["a@example..com"], []),
    ],
)
async def test_malformed_recipient_moves_to_manual_without_send(
    draft_to,
    draft_cc,
):
    dependencies, ctx, db = _runtime()

    with patch("src.init_app.get_app_context", return_value=ctx):
        result = await send_final_email(
            _state(draft_to=draft_to, draft_cc=draft_cc),
            dependencies,
        )

    _assert_manual_delta(result, code="recipient_resolution_failed")
    assert db.status == "manual_review"
    ctx.exchange_client.reply_email.assert_not_awaited()
    ctx.exchange_client.forward_email.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "remote_result",
    [
        False,
        asyncio.TimeoutError("timeout detail must stay local"),
        RuntimeError("remote detail must stay local"),
    ],
    ids=["false", "timeout", "exception"],
)
async def test_ambiguous_remote_outcome_moves_to_manual_and_is_not_retried(
    remote_result,
):
    dependencies, ctx, db = _runtime(remote_result=remote_result)
    state = _state()

    with patch("src.init_app.get_app_context", return_value=ctx):
        first = await send_final_email(deepcopy(state), dependencies)
        second = await send_final_email(deepcopy(state), dependencies)

    _assert_manual_delta(first, code="send_outcome_unknown")
    assert second["next_step"] == "end"
    assert db.status == "send_unknown"
    assert db.error_message == "send_outcome_unknown"
    assert ctx.exchange_client.reply_email.await_count == 1
    ctx.exchange_client.forward_email.assert_not_awaited()
    ctx.email_processor.process_sent_email.assert_not_called()


@pytest.mark.asyncio
async def test_cancelled_remote_send_is_quarantined_before_cancellation_propagates():
    dependencies, ctx, db = _runtime(remote_result=asyncio.CancelledError())
    state = _state()

    with patch("src.init_app.get_app_context", return_value=ctx):
        with pytest.raises(asyncio.CancelledError):
            await send_final_email(deepcopy(state), dependencies)
        second = await send_final_email(deepcopy(state), dependencies)

    assert db.status == "send_unknown"
    assert db.error_message == "send_outcome_unknown"
    assert second == {"next_step": "end"}
    assert ctx.exchange_client.reply_email_result.await_count == 1
    ctx.exchange_client.forward_email_result.assert_not_awaited()


@pytest.mark.asyncio
async def test_success_claims_sending_then_completes_sent_once():
    dependencies, ctx, db = _runtime()

    with patch("src.init_app.get_app_context", return_value=ctx):
        result = await send_final_email(_state(), dependencies)

    assert result == {"next_step": "end"}
    assert db.status == "sent"
    assert [
        (transition["before"], transition["target"], transition["won"])
        for transition in db.transitions
    ] == [
        ("approved", "sending", True),
        ("sending", "sent", True),
    ]
    ctx.exchange_client.reply_email.assert_awaited_once()
    ctx.email_processor.process_sent_email.assert_called_once()


@pytest.mark.asyncio
async def test_remote_success_with_completion_cas_loss_never_sends_again():
    dependencies, ctx, db = _runtime(fail_targets={"sent"})
    state = _state()

    with patch("src.init_app.get_app_context", return_value=ctx):
        first = await send_final_email(deepcopy(state), dependencies)
        second = await send_final_email(deepcopy(state), dependencies)

    _assert_manual_delta(first, code="send_outcome_unknown")
    assert second["next_step"] == "end"
    assert db.status == "send_unknown"
    assert db.error_message == "send_outcome_unknown"
    assert ctx.exchange_client.reply_email.await_count == 1
    ctx.exchange_client.forward_email.assert_not_awaited()


@pytest.mark.asyncio
async def test_send_claim_commit_then_raise_does_not_misattribute_or_send():
    dependencies, ctx, _db = _runtime()

    class CommitThenRaiseClaimDB(LockBackedFakeDB):
        async def compare_and_set_status(self, email_id, *, expected, target):
            result = await super().compare_and_set_status(
                email_id, expected=expected, target=target
            )
            if target == "sending":
                raise DatabaseOperationError(
                    operation="compare_and_set_status",
                    retryable=True,
                    message="bounded claim ambiguity",
                )
            return result

    db = CommitThenRaiseClaimDB()
    ctx.db_manager = db

    with patch("src.init_app.get_app_context", return_value=ctx):
        result = await send_final_email(_state(), dependencies)

    assert result == {"next_step": "end"}
    assert db.status == "sending"
    ctx.exchange_client.reply_email.assert_not_awaited()
    ctx.exchange_client.forward_email.assert_not_awaited()


@pytest.mark.asyncio
async def test_send_completion_commit_then_raise_is_confirmed_by_readback():
    dependencies, ctx, _db = _runtime()

    class CommitThenRaiseCompletionDB(LockBackedFakeDB):
        async def compare_and_set_status(self, email_id, *, expected, target):
            result = await super().compare_and_set_status(
                email_id, expected=expected, target=target
            )
            if target == "sent":
                raise DatabaseOperationError(
                    operation="compare_and_set_status",
                    retryable=True,
                    message="bounded completion ambiguity",
                )
            return result

    db = CommitThenRaiseCompletionDB()
    ctx.db_manager = db

    with patch("src.init_app.get_app_context", return_value=ctx):
        result = await send_final_email(_state(), dependencies)

    assert result == {"next_step": "end"}
    assert db.status == "sent"
    ctx.exchange_client.reply_email.assert_awaited_once()


@pytest.mark.asyncio
async def test_send_completion_raise_before_commit_moves_unknown_to_manual():
    dependencies, ctx, _db = _runtime()

    class RaiseBeforeCompletionDB(LockBackedFakeDB):
        async def compare_and_set_status(self, email_id, *, expected, target):
            if target == "sent":
                raise DatabaseOperationError(
                    operation="compare_and_set_status",
                    retryable=True,
                    message="bounded completion ambiguity",
                )
            return await super().compare_and_set_status(
                email_id, expected=expected, target=target
            )

    db = RaiseBeforeCompletionDB()
    ctx.db_manager = db

    with patch("src.init_app.get_app_context", return_value=ctx):
        result = await send_final_email(_state(), dependencies)

    _assert_manual_delta(result, code="send_outcome_unknown")
    assert db.status == "send_unknown"
    ctx.exchange_client.reply_email.assert_awaited_once()


@pytest.mark.asyncio
async def test_lark_recipient_resolution_does_not_block_event_loop():
    dependencies, ctx, _db = _runtime()
    event_loop_progressed = asyncio.Event()
    observed_progress = []

    def blocking_lookup(_request):
        time.sleep(0.05)
        observed_progress.append(event_loop_progressed.is_set())
        return SimpleNamespace(
            success=lambda: True,
            data=SimpleNamespace(
                user=SimpleNamespace(
                    enterprise_email="resolved@example.com",
                    email=None,
                )
            ),
        )

    client = SimpleNamespace(
        contact=SimpleNamespace(
            v3=SimpleNamespace(
                user=SimpleNamespace(get=blocking_lookup),
            )
        )
    )

    async def tick():
        await asyncio.sleep(0)
        event_loop_progressed.set()

    with patch("src.init_app.get_app_context", return_value=ctx), patch(
        "src.utils.lark_app.lark_api_client",
        client,
    ):
        result, _ = await asyncio.gather(
            send_final_email(
                _state(draft_to=["open_id=recipient-user"]),
                dependencies,
            ),
            tick(),
        )

    assert result == {"next_step": "end"}
    assert observed_progress == [True]
    ctx.exchange_client.reply_email.assert_awaited_once()


@pytest.mark.asyncio
async def test_sender_failure_delta_and_logs_never_contain_sensitive_payloads(caplog):
    dependencies, ctx, db = _runtime(
        remote_result=RuntimeError(EXCEPTION_SENTINEL)
    )

    with caplog.at_level(logging.DEBUG), patch(
        "src.init_app.get_app_context", return_value=ctx
    ):
        result = await send_final_email(_state(), dependencies)

    _assert_manual_delta(result, code="send_outcome_unknown")
    serialized = json.dumps(result, ensure_ascii=False, sort_keys=True)
    for sentinel in (BODY_SENTINEL, DRAFT_SENTINEL, EXCEPTION_SENTINEL):
        assert sentinel not in serialized
        assert sentinel not in caplog.text
    assert db.status == "send_unknown"
    assert db.error_message == "send_outcome_unknown"
