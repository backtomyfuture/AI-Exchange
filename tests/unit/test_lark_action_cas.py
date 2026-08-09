from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.graph.dependencies import GraphDependencies
from src.utils import lark_app


class ActionDB:
    def __init__(self, status: str = "waiting_approval") -> None:
        self.status = status
        self.error_message: str | None = None
        self.transitions: list[tuple[str, frozenset[str], str]] = []
        self.metadata_updates: list[tuple[str, str | None, dict]] = []
        self._lock = asyncio.Lock()

    async def compare_and_set_status(
        self,
        email_id: str,
        *,
        expected: frozenset[str],
        target: str,
    ) -> bool:
        async with self._lock:
            self.transitions.append((email_id, expected, target))
            if self.status not in expected:
                return False
            self.status = target
            return True

    async def update_status(
        self,
        email_id: str,
        status: str | None,
        **kwargs,
    ) -> None:
        self.metadata_updates.append((email_id, status, kwargs))
        if status is not None:
            self.status = status

    async def get_email_status(self, email_id: str) -> str:
        del email_id
        return self.status

    async def compare_and_set_manual_review(
        self,
        email_id: str,
        *,
        expected: frozenset[str],
        error_code: str,
    ) -> bool:
        async with self._lock:
            self.transitions.append((email_id, expected, "manual_review"))
            if self.status not in expected:
                return False
            self.status = "manual_review"
            self.error_message = error_code
            return True

    async def compare_and_set_send_unknown(
        self,
        email_id: str,
        *,
        error_code: str,
    ) -> bool:
        async with self._lock:
            self.transitions.append(
                (email_id, frozenset({"sending"}), "send_unknown")
            )
            if self.status != "sending":
                return False
            self.status = "send_unknown"
            self.error_message = error_code
            return True


class ActionDraftStore:
    def __init__(self, db: ActionDB, value: str = "FINAL-DRAFT") -> None:
        self.db = db
        self.value = value
        self.loads: list[str] = []
        self.conditional_saves: list[tuple[str, str]] = []

    async def save_draft(self, email_id: str, content: str) -> str:
        raise AssertionError((email_id, content, "unconditional save forbidden"))

    async def save_draft_if_status(self, email_id: str, content: str) -> bool:
        self.conditional_saves.append((email_id, content))
        if self.db.status != "waiting_approval":
            return False
        self.value = content
        return True

    async def load_draft(self, draft_id: str) -> str:
        self.loads.append(draft_id)
        return self.value


def _state() -> SimpleNamespace:
    return SimpleNamespace(
        values={
            "email_id": "mail-action",
            "email": {"subject": "small subject"},
            "draft_id": "mail-action",
            "draft_to": ["recipient@example.com"],
            "draft_cc": [],
            "approval_status": "pending",
            "next_step": "",
        }
    )


def _event(action: str, *, options=None, form_value=None) -> SimpleNamespace:
    return SimpleNamespace(
        event=SimpleNamespace(
            action=SimpleNamespace(
                value={"action": action, "id": "mail-action"},
                options=options,
                option=None,
                form_value=form_value or {},
            ),
            operator=SimpleNamespace(open_id="user-action"),
            context=SimpleNamespace(open_message_id="message-action"),
        )
    )


@pytest.fixture
def action_runtime(monkeypatch):
    db = ActionDB()
    drafts = ActionDraftStore(db)
    state = _state()
    graph = SimpleNamespace(
        aget_state=AsyncMock(return_value=state),
        aupdate_state=AsyncMock(),
        ainvoke=AsyncMock(),
    )
    dependencies = GraphDependencies(
        content_store=MagicMock(),
        drafts=drafts,
    )
    monkeypatch.setattr(lark_app, "db_manager", db)
    monkeypatch.setattr(lark_app, "graph", graph)
    monkeypatch.setattr(lark_app, "graph_dependencies", dependencies)
    monkeypatch.setattr(
        lark_app,
        "get_settings",
        lambda: SimpleNamespace(
            DEBUG=False,
            LARK_ALLOWED_OPEN_IDS="user-action",
        ),
    )
    return db, drafts, state, graph


@pytest.mark.asyncio
async def test_duplicate_approval_has_one_setup_winner(action_runtime):
    db, drafts, _state_value, graph = action_runtime

    outcomes = await asyncio.gather(
        lark_app._process_approval_action("mail-action", "user-a"),
        lark_app._process_approval_action("mail-action", "user-b"),
    )

    assert sum(outcome is not None for outcome in outcomes) == 1
    assert db.status == "approved"
    assert drafts.loads == ["mail-action"]
    assert graph.aget_state.await_count == 1
    assert graph.aupdate_state.await_count == 1
    assert len(db.metadata_updates) == 1


@pytest.mark.asyncio
async def test_approval_and_rejection_race_has_one_terminal_setup(action_runtime):
    db, drafts, _state_value, graph = action_runtime

    outcomes = await asyncio.gather(
        lark_app._process_approval_action("mail-action", "approver"),
        lark_app._process_rejection_action(
            "mail-action",
            "rejector",
            "bounded reason",
        ),
    )

    assert sum(outcome is not None for outcome in outcomes) == 1
    assert db.status in {"approved", "rejected"}
    assert graph.aget_state.await_count == 1
    assert graph.aupdate_state.await_count == 1
    assert len(db.metadata_updates) == 1
    assert len(drafts.loads) == (1 if db.status == "approved" else 0)


@pytest.mark.asyncio
async def test_approval_cas_loser_does_not_load_or_mutate_graph(action_runtime):
    db, drafts, _state_value, graph = action_runtime
    db.status = "approved"

    outcome = await lark_app._process_approval_action(
        "mail-action",
        "late-user",
    )

    assert outcome is None
    assert drafts.loads == []
    graph.aget_state.assert_not_awaited()
    graph.aupdate_state.assert_not_awaited()
    graph.ainvoke.assert_not_awaited()
    assert db.metadata_updates == []


@pytest.mark.asyncio
async def test_approval_setup_failure_moves_claim_to_manual(action_runtime):
    db, drafts, _state_value, graph = action_runtime
    graph.aget_state.side_effect = RuntimeError("PRIVATE-FAILURE-DETAIL")

    outcome = await lark_app._process_approval_action(
        "mail-action",
        "approver",
    )

    assert outcome is None
    assert db.status == "manual_review"
    assert db.error_message == "approval_handoff_failed"
    assert drafts.loads == []
    graph.aupdate_state.assert_not_awaited()
    graph.ainvoke.assert_not_awaited()


@pytest.mark.asyncio
async def test_body_edit_is_conditional_and_graph_keeps_only_draft_id(
    action_runtime,
):
    db, drafts, _state_value, graph = action_runtime

    saved = await lark_app._process_modification_action(
        "mail-action",
        "EDITED-DRAFT-SENTINEL",
    )

    assert saved is True
    assert db.status == "waiting_approval"
    assert drafts.conditional_saves == [
        ("mail-action", "EDITED-DRAFT-SENTINEL")
    ]
    update = graph.aupdate_state.await_args.args[1]
    assert update == {"draft_id": "mail-action"}
    assert "EDITED-DRAFT-SENTINEL" not in str(update)
    assert db.metadata_updates == []


@pytest.mark.asyncio
async def test_body_edit_after_approval_has_no_graph_write(action_runtime):
    db, drafts, _state_value, graph = action_runtime
    db.status = "approved"

    saved = await lark_app._process_modification_action(
        "mail-action",
        "TOO-LATE-DRAFT",
    )

    assert saved is False
    assert drafts.conditional_saves == [("mail-action", "TOO-LATE-DRAFT")]
    graph.aget_state.assert_not_awaited()
    graph.aupdate_state.assert_not_awaited()


@pytest.mark.asyncio
async def test_edit_and_approval_share_the_same_email_action_lock(
    action_runtime,
    monkeypatch,
):
    db, _drafts, _state_value, _graph = action_runtime
    started = asyncio.Event()
    release = asyncio.Event()

    class BlockingDraftStore(ActionDraftStore):
        async def save_draft_if_status(
            self,
            email_id: str,
            content: str,
        ) -> bool:
            started.set()
            await release.wait()
            return await super().save_draft_if_status(email_id, content)

    drafts = BlockingDraftStore(db)
    monkeypatch.setattr(
        lark_app,
        "graph_dependencies",
        GraphDependencies(content_store=MagicMock(), drafts=drafts),
    )

    edit = asyncio.create_task(
        lark_app._process_modification_action(
            "mail-action",
            "LOCKED-EDIT",
        )
    )
    await started.wait()
    approval = asyncio.create_task(
        lark_app._process_approval_action("mail-action", "approver")
    )
    await asyncio.sleep(0)

    assert db.transitions == []
    release.set()

    assert await edit is True
    assert await approval is not None
    assert db.status == "approved"
    assert drafts.loads == ["mail-action"]
    assert db.metadata_updates[-1][2]["final_draft"] == "LOCKED-EDIT"


@pytest.mark.asyncio
async def test_recipient_mutation_after_approval_is_rejected(action_runtime):
    db, _drafts, _state_value, graph = action_runtime
    db.status = "approved"

    saved = await lark_app._update_recipient_field_if_waiting(
        "mail-action",
        {"draft_to": ["late@example.com"]},
    )

    assert saved is False
    graph.aget_state.assert_not_awaited()
    graph.aupdate_state.assert_not_awaited()


def _close_and_return(
    coroutine: Coroutine[object, object, object],
    result: object,
) -> object:
    coroutine.close()
    return result


def test_approval_wrapper_schedules_resume_only_for_winner(monkeypatch):
    handoff = (_state(), {"configurable": {"thread_id": "mail-action"}})
    outcomes = iter((handoff, None))
    scheduled: list[Coroutine] = []

    monkeypatch.setattr(
        lark_app,
        "safe_async_wait",
        lambda coroutine: _close_and_return(coroutine, next(outcomes)),
    )

    def capture(coroutine):
        scheduled.append(coroutine)
        coroutine.close()

    monkeypatch.setattr(lark_app, "safe_async_run", capture)
    assert lark_app.process_approval("mail-action", "user-a") is True
    assert lark_app.process_approval("mail-action", "user-b") is False
    assert len(scheduled) == 1


def test_approval_schedule_failure_moves_claim_to_manual(
    action_runtime,
    monkeypatch,
):
    db, _drafts, _state_value, graph = action_runtime

    def fail_schedule(coroutine):
        coroutine.close()
        raise RuntimeError("PRIVATE-SCHEDULE-FAILURE")

    monkeypatch.setattr(lark_app, "safe_async_run", fail_schedule)
    monkeypatch.setattr(lark_app, "worker_loop", None)

    assert lark_app.process_approval("mail-action", "approver") is False
    assert db.status == "manual_review"
    assert db.error_message == "approval_handoff_failed"
    graph.ainvoke.assert_not_awaited()


@pytest.mark.asyncio
async def test_scheduled_approval_resume_failure_moves_runtime_state_to_manual(
    action_runtime,
):
    db, _drafts, state, graph = action_runtime
    db.status = "approved"
    graph.ainvoke.side_effect = RuntimeError("PRIVATE-ASYNC-FAILURE")
    config = {"configurable": {"thread_id": "mail-action"}}

    await lark_app._resume_graph_then_cleanup("mail-action", state, config)

    assert db.status == "manual_review"
    assert db.error_message == "approval_handoff_failed"


@pytest.mark.asyncio
async def test_approval_claim_commit_then_raise_is_not_misattributed_or_resumed(
    action_runtime,
    monkeypatch,
):
    db, drafts, _state, graph = action_runtime

    async def ambiguous_claim(*_args, **_kwargs):
        db.status = "approved"
        raise RuntimeError("PRIVATE-CLAIM-AMBIGUITY")

    monkeypatch.setattr(lark_app, "claim_approval", ambiguous_claim)

    result = await lark_app._process_approval_action("mail-action", "approver-1")

    assert result is None
    assert db.status == "approved"
    assert db.error_message is None
    assert drafts.loads == []
    graph.aget_state.assert_not_awaited()


@pytest.mark.asyncio
async def test_draft_claim_commit_then_raise_never_starts_remote_save(
    action_runtime,
    monkeypatch,
):
    db, _drafts, _state, _graph = action_runtime

    async def ambiguous_claim(*_args, **_kwargs):
        db.status = "saving_draft"
        raise RuntimeError("PRIVATE-CLAIM-AMBIGUITY")

    monkeypatch.setattr(lark_app, "claim_draft_save", ambiguous_claim)

    claimed = await lark_app._claim_draft_save_action("mail-action")

    assert claimed is False
    assert db.status == "saving_draft"
    assert db.error_message is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("action", "claimed_status"),
    [("approval", "approved"), ("rejection", "rejected")],
)
async def test_owned_action_handoff_cancellation_moves_manual_before_reraise(
    action_runtime,
    monkeypatch,
    action,
    claimed_status,
):
    db, _drafts, _state, graph = action_runtime
    graph.aget_state.side_effect = asyncio.CancelledError()
    monkeypatch.setattr(lark_app, "db_manager", db)
    monkeypatch.setattr(lark_app, "graph", graph)

    with pytest.raises(asyncio.CancelledError):
        if action == "approval":
            await lark_app._process_approval_action("mail-action", "operator")
        else:
            await lark_app._process_rejection_action("mail-action", "operator")

    assert any(
        transition[2] == claimed_status for transition in db.transitions
    )
    assert db.status == "manual_review"
    assert db.error_message == "approval_handoff_failed"


def test_stale_approval_handler_does_not_hydrate_or_report_success(
    action_runtime,
    monkeypatch,
):
    db, _drafts, _state_value, graph = action_runtime
    db.status = "approved"
    hydrate = AsyncMock(
        return_value=(
            {"subject": "hydration must not happen"},
            "PRIVATE-DRAFT",
        )
    )
    monkeypatch.setattr(lark_app, "_hydrate_lark_projection", hydrate)
    monkeypatch.setattr(lark_app, "card_builder", MagicMock())
    monkeypatch.setattr(lark_app, "worker_loop", None)

    result = lark_app.handle_card_action(_event("approve"))

    hydrate.assert_not_awaited()
    graph.aupdate_state.assert_not_awaited()
    graph.ainvoke.assert_not_awaited()
    assert result["toast"]["type"] != "success"


def test_stale_body_edit_handler_does_not_hydrate_or_claim_success(
    action_runtime,
    monkeypatch,
):
    db, _drafts, _state_value, graph = action_runtime
    db.status = "approved"
    hydrate = AsyncMock(
        return_value=(
            {"subject": "hydration must not happen"},
            "PRIVATE-DRAFT",
        )
    )
    monkeypatch.setattr(lark_app, "_hydrate_lark_projection", hydrate)
    monkeypatch.setattr(lark_app, "card_builder", MagicMock())
    monkeypatch.setattr(lark_app, "worker_loop", None)

    result = lark_app.handle_card_action(
        _event("save_draft", form_value={"draft_input": "TOO-LATE"})
    )

    hydrate.assert_not_awaited()
    graph.aupdate_state.assert_not_awaited()
    assert result["toast"]["type"] != "success"


def test_stale_save_draft_only_handler_has_no_side_effect(
    action_runtime,
    monkeypatch,
):
    db, _drafts, state, graph = action_runtime
    db.status = "sent"
    hydrate = AsyncMock(
        return_value=(
            {"subject": "hydration must not happen"},
            "PRIVATE-DRAFT",
        )
    )
    create_draft = AsyncMock(return_value=True)
    schedule = MagicMock()
    monkeypatch.setattr(lark_app, "_hydrate_lark_projection", hydrate)
    monkeypatch.setattr(
        lark_app,
        "exchange_client",
        SimpleNamespace(create_draft=create_draft),
    )
    monkeypatch.setattr(lark_app, "safe_async_run", schedule)
    monkeypatch.setattr(lark_app, "worker_loop", None)

    result = lark_app.handle_card_action(_event("save_draft_only"))

    hydrate.assert_not_awaited()
    create_draft.assert_not_awaited()
    schedule.assert_not_called()
    graph.aupdate_state.assert_not_awaited()
    assert db.status == "sent"
    assert state.values["approval_status"] == "pending"
    assert result["toast"]["type"] != "success"


def test_duplicate_save_draft_only_claims_and_schedules_once(
    action_runtime,
    monkeypatch,
):
    db, _drafts, _state_value, _graph = action_runtime
    scheduled: list[Coroutine] = []

    def capture(coroutine):
        scheduled.append(coroutine)
        coroutine.close()

    monkeypatch.setattr(lark_app, "safe_async_run", capture)
    monkeypatch.setattr(lark_app, "worker_loop", None)

    first = lark_app.handle_card_action(_event("save_draft_only"))
    second = lark_app.handle_card_action(_event("save_draft_only"))

    assert first["toast"]["type"] == "info"
    assert second["toast"]["type"] == "warning"
    assert db.status == "saving_draft"
    assert len(scheduled) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(("remote_result", "expected"), [(True, True), (False, False)])
async def test_claimed_exchange_draft_save_completes_or_moves_manual(
    action_runtime,
    monkeypatch,
    remote_result,
    expected,
):
    db, _drafts, state, _graph = action_runtime
    db.status = "saving_draft"
    create_draft = AsyncMock(return_value=remote_result)
    cleanup = AsyncMock()
    monkeypatch.setattr(
        lark_app,
        "_hydrate_lark_projection",
        AsyncMock(
            return_value=(
                {
                    "subject": "subject",
                    "draft_to": ["recipient@example.com"],
                    "draft_cc": [],
                },
                "DRAFT-BODY",
            )
        ),
    )
    monkeypatch.setattr(
        lark_app,
        "exchange_client",
        SimpleNamespace(create_draft=create_draft),
    )
    monkeypatch.setattr(lark_app, "_cleanup_action_drive_tokens", cleanup)

    result = await lark_app._process_claimed_draft_save("mail-action", state)

    assert result is expected
    create_draft.assert_awaited_once()
    if expected:
        assert db.status == "draft_saved"
        cleanup.assert_awaited_once_with("mail-action", state)
    else:
        assert db.status == "manual_review"
        assert db.error_message == "draft_save_outcome_unknown"
        cleanup.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("error_type", [TimeoutError, RuntimeError])
async def test_claimed_exchange_draft_exception_moves_manual_without_logging_detail(
    action_runtime,
    monkeypatch,
    caplog,
    error_type,
):
    db, _drafts, state, _graph = action_runtime
    db.status = "saving_draft"
    sensitive_detail = "PRIVATE-DRAFT-SAVE-SENTINEL"
    create_draft = AsyncMock(side_effect=error_type(sensitive_detail))
    cleanup = AsyncMock()
    monkeypatch.setattr(
        lark_app,
        "_hydrate_lark_projection",
        AsyncMock(
            return_value=(
                {
                    "subject": "subject",
                    "draft_to": ["recipient@example.com"],
                    "draft_cc": [],
                },
                "DRAFT-BODY",
            )
        ),
    )
    monkeypatch.setattr(
        lark_app,
        "exchange_client",
        SimpleNamespace(create_draft=create_draft),
    )
    monkeypatch.setattr(lark_app, "_cleanup_action_drive_tokens", cleanup)
    caplog.set_level("ERROR", logger=lark_app.__name__)

    result = await lark_app._process_claimed_draft_save("mail-action", state)

    assert result is False
    assert db.status == "manual_review"
    assert db.error_message == "draft_save_outcome_unknown"
    create_draft.assert_awaited_once()
    cleanup.assert_not_awaited()
    assert error_type.__name__ in caplog.text
    assert sensitive_detail not in caplog.text


@pytest.mark.asyncio
async def test_exchange_draft_success_with_lost_completion_cas_moves_manual(
    action_runtime,
    monkeypatch,
):
    db, _drafts, state, _graph = action_runtime
    db.status = "saving_draft"
    create_draft = AsyncMock(return_value=True)
    complete = AsyncMock(return_value=False)
    cleanup = AsyncMock()
    monkeypatch.setattr(
        lark_app,
        "_hydrate_lark_projection",
        AsyncMock(return_value=({"subject": "subject"}, "DRAFT-BODY")),
    )
    monkeypatch.setattr(
        lark_app,
        "exchange_client",
        SimpleNamespace(create_draft=create_draft),
    )
    monkeypatch.setattr(lark_app, "complete_draft_save", complete)
    monkeypatch.setattr(lark_app, "_cleanup_action_drive_tokens", cleanup)

    result = await lark_app._process_claimed_draft_save("mail-action", state)

    assert result is False
    assert db.status == "manual_review"
    assert db.error_message == "draft_save_outcome_unknown"
    create_draft.assert_awaited_once()
    complete.assert_awaited_once_with("mail-action", db)
    cleanup.assert_not_awaited()


@pytest.mark.asyncio
async def test_exchange_draft_completion_commit_then_raise_uses_readback(
    action_runtime,
    monkeypatch,
):
    db, _drafts, state, _graph = action_runtime
    db.status = "saving_draft"
    create_draft = AsyncMock(return_value=True)
    cleanup = AsyncMock()

    async def ambiguous_completion(*_args, **_kwargs):
        db.status = "draft_saved"
        raise RuntimeError("PRIVATE-DRAFT-COMPLETION")

    monkeypatch.setattr(
        lark_app,
        "_hydrate_lark_projection",
        AsyncMock(return_value=({"subject": "subject"}, "DRAFT-BODY")),
    )
    monkeypatch.setattr(
        lark_app,
        "exchange_client",
        SimpleNamespace(create_draft=create_draft),
    )
    monkeypatch.setattr(lark_app, "complete_draft_save", ambiguous_completion)
    monkeypatch.setattr(lark_app, "_cleanup_action_drive_tokens", cleanup)

    result = await lark_app._process_claimed_draft_save("mail-action", state)

    assert result is True
    assert db.status == "draft_saved"
    create_draft.assert_awaited_once()
    cleanup.assert_awaited_once_with("mail-action", state)


@pytest.mark.asyncio
async def test_exchange_draft_completion_raise_before_commit_moves_manual(
    action_runtime,
    monkeypatch,
):
    db, _drafts, state, _graph = action_runtime
    db.status = "saving_draft"
    create_draft = AsyncMock(return_value=True)
    cleanup = AsyncMock()
    monkeypatch.setattr(
        lark_app,
        "_hydrate_lark_projection",
        AsyncMock(return_value=({"subject": "subject"}, "DRAFT-BODY")),
    )
    monkeypatch.setattr(
        lark_app,
        "exchange_client",
        SimpleNamespace(create_draft=create_draft),
    )
    monkeypatch.setattr(
        lark_app,
        "complete_draft_save",
        AsyncMock(side_effect=RuntimeError("PRIVATE-DRAFT-COMPLETION")),
    )
    monkeypatch.setattr(lark_app, "_cleanup_action_drive_tokens", cleanup)

    result = await lark_app._process_claimed_draft_save("mail-action", state)

    assert result is False
    assert db.status == "manual_review"
    assert db.error_message == "draft_save_outcome_unknown"
    create_draft.assert_awaited_once()
    cleanup.assert_not_awaited()


@pytest.mark.asyncio
async def test_second_exchange_draft_callback_does_not_create_another_draft(
    action_runtime,
    monkeypatch,
):
    db, _drafts, state, _graph = action_runtime
    create_draft = AsyncMock(return_value=True)
    hydrate = AsyncMock(return_value=({"subject": "subject"}, "DRAFT-BODY"))
    cleanup = AsyncMock()
    monkeypatch.setattr(lark_app, "_hydrate_lark_projection", hydrate)
    monkeypatch.setattr(
        lark_app,
        "exchange_client",
        SimpleNamespace(create_draft=create_draft),
    )
    monkeypatch.setattr(lark_app, "_cleanup_action_drive_tokens", cleanup)

    first = await lark_app.process_save_draft("mail-action", state)
    second = await lark_app.process_save_draft("mail-action", state)

    assert first is True
    assert second is False
    assert db.status == "draft_saved"
    create_draft.assert_awaited_once()
    hydrate.assert_awaited_once_with(state)
    cleanup.assert_awaited_once_with("mail-action", state)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("raw_to", "raw_cc"),
    [
        ([], []),
        (["recipient@example.com,attacker@example.com"], []),
        (["recipient@example.com\nBcc: attacker@example.com"], []),
        (["recipient@example.com"], ["not-an-address"]),
        ("recipient@example.com", []),
    ],
)
async def test_invalid_draft_recipients_never_reach_exchange(
    action_runtime,
    monkeypatch,
    raw_to,
    raw_cc,
):
    db, _drafts, state, _graph = action_runtime
    db.status = "saving_draft"
    state.values["draft_to"] = raw_to
    state.values["draft_cc"] = raw_cc
    create_draft = AsyncMock(return_value=True)
    cleanup = AsyncMock()
    monkeypatch.setattr(
        lark_app,
        "_hydrate_lark_projection",
        AsyncMock(return_value=({"subject": "subject"}, "DRAFT-BODY")),
    )
    monkeypatch.setattr(
        lark_app,
        "exchange_client",
        SimpleNamespace(create_draft=create_draft),
    )
    monkeypatch.setattr(lark_app, "_cleanup_action_drive_tokens", cleanup)

    result = await lark_app._process_claimed_draft_save("mail-action", state)

    assert result is False
    assert db.status == "manual_review"
    assert db.error_message == "recipient_resolution_failed"
    create_draft.assert_not_awaited()
    cleanup.assert_not_awaited()


@pytest.mark.parametrize(
    ("action", "options", "form_value"),
    [
        ("select_to", ["late-user"], None),
        ("select_cc", ["late-user"], None),
        (
            "save_to",
            None,
            {
                "to_existing": [],
                "to_new": ["late-user"],
                "to_external_input": "",
            },
        ),
        (
            "save_cc",
            None,
            {
                "cc_existing": [],
                "cc_new": ["late-user"],
                "cc_external_input": "",
            },
        ),
    ],
)
def test_stale_recipient_mutation_handler_does_not_hydrate_or_write(
    action_runtime,
    monkeypatch,
    action,
    options,
    form_value,
):
    db, _drafts, _state_value, graph = action_runtime
    db.status = "approved"
    hydrate = AsyncMock(
        return_value=(
            {"subject": "hydration must not happen"},
            "PRIVATE-DRAFT",
        )
    )
    monkeypatch.setattr(lark_app, "_hydrate_lark_projection", hydrate)
    monkeypatch.setattr(lark_app, "card_builder", MagicMock())
    monkeypatch.setattr(lark_app, "worker_loop", None)

    result = lark_app.handle_card_action(
        _event(action, options=options, form_value=form_value)
    )

    hydrate.assert_not_awaited()
    graph.aupdate_state.assert_not_awaited()
    assert result["toast"]["type"] != "success"


@pytest.mark.asyncio
async def test_safe_async_wait_rejects_same_worker_loop_without_deadlock(
    monkeypatch,
):
    loop = asyncio.get_running_loop()
    monkeypatch.setattr(lark_app, "worker_loop", loop)

    async def operation():
        return True

    with pytest.raises(RuntimeError, match="safe_async_wait_same_loop"):
        lark_app.safe_async_wait(operation())


@pytest.mark.asyncio
async def test_safe_async_wait_rejects_running_loop_fallback_without_warning(
    monkeypatch,
):
    monkeypatch.setattr(lark_app, "worker_loop", None)

    async def operation():
        return True

    with pytest.raises(RuntimeError, match="safe_async_wait_running_loop"):
        lark_app.safe_async_wait(operation())


@pytest.mark.asyncio
async def test_safe_async_run_returns_observed_task_and_bounds_failure_log(
    monkeypatch,
    caplog,
):
    monkeypatch.setattr(lark_app, "worker_loop", None)
    sensitive = "PRIVATE-BACKGROUND-FAILURE"

    async def operation():
        raise RuntimeError(sensitive)

    caplog.set_level("ERROR", logger=lark_app.__name__)
    task = lark_app.safe_async_run(operation())
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert isinstance(task, asyncio.Task)
    assert "RuntimeError" in caplog.text
    assert sensitive not in caplog.text


@pytest.mark.asyncio
async def test_cancelled_approval_resume_moves_claimed_row_to_manual(
    action_runtime,
    monkeypatch,
):
    db, _drafts, state, graph = action_runtime
    db.status = "approved"
    graph.ainvoke.side_effect = asyncio.CancelledError()
    monkeypatch.setattr(lark_app, "db_manager", db)
    monkeypatch.setattr(lark_app, "graph", graph)

    with pytest.raises(asyncio.CancelledError):
        await lark_app._resume_graph_then_cleanup(
            "mail-action",
            state,
            {"configurable": {"thread_id": "mail-action"}},
        )

    assert db.status == "manual_review"
    assert db.error_message == "approval_handoff_failed"


@pytest.mark.asyncio
async def test_normal_graph_return_does_not_cleanup_unconfirmed_sending_state(
    action_runtime,
    monkeypatch,
):
    db, _drafts, state, graph = action_runtime
    db.status = "sending"
    cleanup = AsyncMock()
    monkeypatch.setattr(lark_app, "db_manager", db)
    monkeypatch.setattr(lark_app, "graph", graph)
    monkeypatch.setattr(lark_app, "_cleanup_action_drive_tokens", cleanup)

    await lark_app._resume_graph_then_cleanup(
        "mail-action",
        state,
        {"configurable": {"thread_id": "mail-action"}},
    )

    cleanup.assert_not_awaited()


@pytest.mark.asyncio
async def test_normal_graph_return_cleans_confirmed_sent_state(
    action_runtime,
    monkeypatch,
):
    db, _drafts, state, graph = action_runtime
    db.status = "sent"
    cleanup = AsyncMock()
    monkeypatch.setattr(lark_app, "db_manager", db)
    monkeypatch.setattr(lark_app, "graph", graph)
    monkeypatch.setattr(lark_app, "_cleanup_action_drive_tokens", cleanup)

    await lark_app._resume_graph_then_cleanup(
        "mail-action",
        state,
        {"configurable": {"thread_id": "mail-action"}},
    )

    cleanup.assert_awaited_once()


@pytest.mark.asyncio
async def test_resume_failure_quarantines_started_send_as_send_unknown(
    action_runtime,
    monkeypatch,
):
    db, _drafts, state, graph = action_runtime
    db.status = "sending"
    graph.ainvoke.side_effect = RuntimeError("private graph failure")
    cleanup = AsyncMock()
    monkeypatch.setattr(lark_app, "db_manager", db)
    monkeypatch.setattr(lark_app, "graph", graph)
    monkeypatch.setattr(lark_app, "_cleanup_action_drive_tokens", cleanup)

    await lark_app._resume_graph_then_cleanup(
        "mail-action",
        state,
        {"configurable": {"thread_id": "mail-action"}},
    )

    assert db.status == "send_unknown"
    assert db.error_message == "send_outcome_unknown"
    cleanup.assert_awaited_once()


@pytest.mark.asyncio
async def test_cancel_during_resume_quarantine_never_cleans_unconfirmed_resources(
    action_runtime,
    monkeypatch,
):
    db, _drafts, state, graph = action_runtime
    db.status = "approved"
    graph.ainvoke.side_effect = asyncio.CancelledError()
    db.get_email_status = AsyncMock(side_effect=asyncio.CancelledError())
    cleanup = AsyncMock()
    monkeypatch.setattr(lark_app, "db_manager", db)
    monkeypatch.setattr(lark_app, "graph", graph)
    monkeypatch.setattr(lark_app, "_cleanup_action_drive_tokens", cleanup)

    with pytest.raises(asyncio.CancelledError):
        await lark_app._resume_graph_then_cleanup(
            "mail-action",
            state,
            {"configurable": {"thread_id": "mail-action"}},
        )

    cleanup.assert_not_awaited()
