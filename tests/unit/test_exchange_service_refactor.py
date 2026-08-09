import ast
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from src.domain.email_state import InitialEmailWriteResult
from src.exchange_service import (
    CleanupHandleSnapshot,
    NotificationPdfStage,
    _dispatch_notification,
    _run_ai_pipeline,
    process_and_archive_email_guarded,
)
from src.ingestion.processing import (
    ExternalEffectAuthorizationError,
    ExternalEffectBoundary,
    GuardedExternalEffectFailed,
    ProcessingEffectScope,
    ProcessingPolicyRejected,
)
from src.storage import ContentRef
from src.utils.lark_pdf_flow import PdfFlowOutcome


@pytest.fixture(autouse=True)
def _stable_guarded_account_setting():
    with patch(
        "src.exchange_service.get_settings",
        return_value=SimpleNamespace(EXCHANGE_ACCOUNT_ID=8),
    ):
        yield


def _get_function_source(path: str, function_name: str) -> str:
    source = Path(path).read_text(encoding="utf-8")
    tree = ast.parse(source)
    lines = source.splitlines()
    for node in tree.body:
        if isinstance(node, ast.AsyncFunctionDef) and node.name == function_name:
            # end_lineno is available on Python 3.8+
            end = getattr(node, "end_lineno", node.lineno)
            return "\n".join(lines[node.lineno - 1 : end])
    raise AssertionError(f"Function {function_name} not found")


def test_process_function_is_short():
    """process_and_archive_email should be under 60 lines after refactor."""
    source = _get_function_source(
        "src/exchange_service.py", "process_and_archive_email"
    )
    line_count = len(source.strip().split("\n"))
    assert line_count < 60, (
        f"process_and_archive_email is {line_count} lines, should be <60"
    )


def test_helper_functions_exist():
    """Verify sub-functions have been extracted."""
    source = Path("src/exchange_service.py").read_text(encoding="utf-8")
    assert "async def _ingest_to_qdrant(" in source, "Missing _ingest_to_qdrant"
    assert "async def _run_ai_pipeline(" in source, "Missing _run_ai_pipeline"
    assert "async def _dispatch_notification(" in source, (
        "Missing _dispatch_notification"
    )
    assert "async def _mark_email_read(" in source, "Missing _mark_email_read"


def _scope(email_id: str, *, account_id: int = 8) -> ProcessingEffectScope:
    return ProcessingEffectScope(
        account_id=account_id,
        inbox_id=str(uuid4()),
        generation=1,
        fencing_token=1,
        attempts=0,
        email_id=str(uuid4()),
        expected_email_version=1,
        event_dedupe_key="a" * 64,
        external_email_id=email_id,
    )


def _guarded_ctx() -> SimpleNamespace:
    ref = ContentRef(
        account_id=8,
        object_id="00000000-0000-4000-8000-000000000088",
        key_version="v1",
        sha256="8" * 64,
    )
    return SimpleNamespace(
        db_manager=SimpleNamespace(
            log_initial_email=AsyncMock(return_value=InitialEmailWriteResult.CREATED),
            set_content_ref_if_absent=AsyncMock(return_value=True),
            get_content_ref=AsyncMock(return_value=ref),
            update_status=AsyncMock(),
        ),
        content_store=SimpleNamespace(
            put_email=AsyncMock(return_value=ref),
            delete=AsyncMock(),
        ),
        email_processor=SimpleNamespace(process_email=MagicMock()),
        exchange_client=SimpleNamespace(mark_as_read=AsyncMock(return_value=True)),
        graph=SimpleNamespace(astream=MagicMock()),
    )


@pytest.mark.asyncio
async def test_guarded_entry_rejects_cross_account_scope_before_any_io() -> None:
    ctx = _guarded_ctx()
    before = AsyncMock(return_value=None)

    with pytest.raises(ProcessingPolicyRejected):
        await process_and_archive_email_guarded(
            {"id": "cross-account", "subject": "s", "sender": "a@example.test"},
            ctx,
            skip_analysis=True,
            before_external_effect=before,
            effect_scope=_scope("cross-account", account_id=9),
        )

    before.assert_not_awaited()
    ctx.db_manager.log_initial_email.assert_not_awaited()
    ctx.content_store.put_email.assert_not_awaited()
    ctx.email_processor.process_email.assert_not_called()
    ctx.exchange_client.mark_as_read.assert_not_awaited()


@pytest.mark.asyncio
async def test_guard_denial_stops_before_qdrant_and_any_later_effect() -> None:
    ctx = _guarded_ctx()
    seen: list[str] = []

    async def deny_qdrant(kind: str, _ordinal: int, _target_hash: str) -> None:
        seen.append(kind)
        if kind == "qdrant":
            raise ExternalEffectAuthorizationError()

    with pytest.raises(ExternalEffectAuthorizationError):
        await process_and_archive_email_guarded(
            {"id": "guard-denied", "subject": "s", "sender": "a@example.test"},
            ctx,
            skip_analysis=True,
            before_external_effect=deny_qdrant,
            effect_scope=_scope("guard-denied"),
        )

    assert seen == ["content", "qdrant"]
    ctx.email_processor.process_email.assert_not_called()
    ctx.db_manager.update_status.assert_not_awaited()
    ctx.exchange_client.mark_as_read.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("remote_result", "remote_error"),
    [
        (None, RuntimeError("remote failed")),
        ({}, None),
    ],
    ids=["raises", "missing-token"],
)
async def test_guarded_remote_failure_stops_all_later_external_calls(
    remote_result: object,
    remote_error: Exception | None,
) -> None:
    ctx = _guarded_ctx()
    seen: list[str] = []

    async def authorize(kind: str, _ordinal: int, _target_hash: str) -> None:
        seen.append(kind)

    email = {
        "id": "guard-remote-failure",
        "subject": "s",
        "sender": "a@example.test",
        "attachments": [{"name": "a.txt", "content": "YQ=="}],
    }
    ctx.email_processor.process_email.return_value = True
    with (
        patch(
            "src.exchange_service._snapshot_cleanup_handles",
            new=AsyncMock(return_value=CleanupHandleSnapshot()),
        ),
        patch(
            "src.exchange_service._checkpoint_ai_path_resources",
            new=AsyncMock(return_value=CleanupHandleSnapshot()),
        ),
        patch(
            "src.exchange_service._run_ai_pipeline",
            new=AsyncMock(return_value=_guarded_pipeline_result(need_reply=True)),
        ),
        patch(
            "src.exchange_service.lark_app.upload_file_to_drive",
            return_value=remote_result,
            side_effect=remote_error,
        ),
    ):
        with pytest.raises(GuardedExternalEffectFailed):
            await process_and_archive_email_guarded(
                email,
                ctx,
                before_external_effect=authorize,
                effect_scope=_scope("guard-remote-failure"),
            )

    assert seen == ["content", "qdrant", "feishu"]
    ctx.email_processor.process_email.assert_called_once_with(email)
    ctx.exchange_client.mark_as_read.assert_not_awaited()
    ctx.content_store.delete.assert_not_awaited()


def _guarded_pipeline_result(*, need_reply: bool) -> dict[str, object]:
    return {
        "classification": {
            "need_reply": need_reply,
            "priority": "P1",
            "intent": "approval" if need_reply else "notice",
        },
        "draft": "draft",
        "context": [],
        "email": {"id": "message-1", "subject": "subject"},
        "routing_log": [],
        "route_decision": {
            "outcome": "matched",
            "route": "read_only",
            "params": {},
            "provenance": {"tier": "system", "source_version": "test-v1"},
            "reason_code": None,
            "selected_action_fingerprint": None,
            "candidate_actions": [],
        },
    }


def _guarded_dispatch_ctx() -> SimpleNamespace:
    return SimpleNamespace(
        db_manager=SimpleNamespace(
            update_status=AsyncMock(),
            persist_route_decision=AsyncMock(),
        ),
        email_processor=SimpleNamespace(
            update_email_labels=MagicMock(return_value=True)
        ),
        graph=SimpleNamespace(
            aget_state=AsyncMock(),
            aupdate_state=AsyncMock(),
        ),
    )


@pytest.mark.parametrize(
    ("remote_result", "remote_error"),
    [
        (False, None),
        (None, RuntimeError("qdrant unavailable")),
    ],
    ids=["false-result", "raises"],
)
@pytest.mark.asyncio
async def test_guarded_label_write_failure_stops_before_any_feishu_call(
    remote_result: object,
    remote_error: Exception | None,
) -> None:
    ctx = _guarded_dispatch_ctx()
    ctx.email_processor.update_email_labels = MagicMock(
        return_value=remote_result,
        side_effect=remote_error,
    )
    before = AsyncMock(return_value=None)
    boundary = ExternalEffectBoundary(_scope("message-1"), before)

    with (
        patch(
            "src.exchange_service.lark_app.generate_and_upload_pdf",
            new=AsyncMock(),
        ) as generate_pdf,
        patch("src.exchange_service.lark_app.send_approval_card") as send_card,
    ):
        with pytest.raises(GuardedExternalEffectFailed):
            await _dispatch_notification(
                "message-1",
                _guarded_pipeline_result(need_reply=True),
                ctx,
                {},
                _effect_boundary=boundary,
            )

    assert [call.args[0] for call in before.await_args_list] == ["qdrant"]
    generate_pdf.assert_not_awaited()
    send_card.assert_not_called()


@pytest.mark.parametrize("need_reply", [True, False], ids=["approval", "read-only"])
@pytest.mark.parametrize(
    ("remote_result", "remote_error"),
    [
        (None, None),
        (None, RuntimeError("pdf unavailable")),
    ],
    ids=["non-mapping", "raises"],
)
@pytest.mark.asyncio
async def test_guarded_pdf_failure_stops_before_card_send(
    need_reply: bool,
    remote_result: object,
    remote_error: Exception | None,
) -> None:
    ctx = _guarded_dispatch_ctx()
    before = AsyncMock(return_value=None)
    boundary = ExternalEffectBoundary(_scope("message-1"), before)
    send_name = "send_approval_card" if need_reply else "send_read_only_card"

    with (
        patch(
            "src.exchange_service.lark_app.generate_and_upload_pdf",
            new=AsyncMock(return_value=remote_result, side_effect=remote_error),
        ),
        patch(f"src.exchange_service.lark_app.{send_name}") as send_card,
    ):
        with pytest.raises(GuardedExternalEffectFailed):
            await _dispatch_notification(
                "message-1",
                _guarded_pipeline_result(need_reply=need_reply),
                ctx,
                {},
                _effect_boundary=boundary,
            )

    assert [call.args[0] for call in before.await_args_list] == [
        "qdrant",
        "feishu",
    ]
    send_card.assert_not_called()


@pytest.mark.parametrize("need_reply", [True, False], ids=["approval", "read-only"])
@pytest.mark.asyncio
async def test_guarded_pdf_outcome_tracks_cleanup_handles_before_fixed_failure(
    need_reply: bool,
) -> None:
    values = {
        "attachment_tokens": [],
        "pdf_token": "protected-pdf",
    }
    state = SimpleNamespace(values=values)

    async def update_state(_config, delta) -> None:
        values.update(delta)

    ctx = _guarded_dispatch_ctx()
    ctx.graph.aget_state = AsyncMock(return_value=state)
    ctx.graph.aupdate_state = AsyncMock(side_effect=update_state)
    before = AsyncMock(return_value=None)
    boundary = ExternalEffectBoundary(_scope("message-1"), before)
    send_name = "send_approval_card" if need_reply else "send_read_only_card"
    outcome = PdfFlowOutcome(
        status="upload_cleanup_required",
        retryable=True,
        cleanup_tokens=("cleanup-pdf",),
        protected_tokens=("protected-pdf",),
    )

    with (
        patch(
            "src.exchange_service.lark_app.generate_and_upload_pdf",
            new=AsyncMock(return_value=outcome),
        ),
        patch(f"src.exchange_service.lark_app.{send_name}") as send_card,
    ):
        with pytest.raises(GuardedExternalEffectFailed):
            await _dispatch_notification(
                "message-1",
                _guarded_pipeline_result(need_reply=need_reply),
                ctx,
                {},
                _effect_boundary=boundary,
            )

    assert values["attachment_tokens"] == ["cleanup-pdf"]
    assert [call.args[0] for call in before.await_args_list] == [
        "qdrant",
        "feishu",
    ]
    send_card.assert_not_called()


@pytest.mark.parametrize("need_reply", [True, False], ids=["approval", "read-only"])
@pytest.mark.parametrize(
    ("remote_result", "remote_error"),
    [
        (False, None),
        (None, RuntimeError("card unavailable")),
    ],
    ids=["false-result", "raises"],
)
@pytest.mark.asyncio
async def test_guarded_card_failure_stops_before_cleanup_or_success_status(
    need_reply: bool,
    remote_result: object,
    remote_error: Exception | None,
) -> None:
    ctx = _guarded_dispatch_ctx()
    before = AsyncMock(return_value=None)
    boundary = ExternalEffectBoundary(_scope("message-1"), before)
    send_name = "send_approval_card" if need_reply else "send_read_only_card"

    with (
        patch(
            "src.exchange_service.lark_app.generate_and_upload_pdf",
            new=AsyncMock(return_value={"file_token": "pdf", "url": "https://pdf"}),
        ),
        patch(
            "src.exchange_service._stage_notification_pdf",
            new=AsyncMock(
                return_value=NotificationPdfStage(
                    ready=True,
                    old_token=None,
                    new_token="pdf",
                    url="https://pdf",
                )
            ),
        ),
        patch(
            f"src.exchange_service.lark_app.{send_name}",
            return_value=remote_result,
            side_effect=remote_error,
        ),
        patch(
            "src.exchange_service._delete_replaced_pdf",
            new=AsyncMock(),
        ) as delete_replaced,
    ):
        with pytest.raises(GuardedExternalEffectFailed):
            await _dispatch_notification(
                "message-1",
                _guarded_pipeline_result(need_reply=need_reply),
                ctx,
                {},
                _effect_boundary=boundary,
            )

    assert [call.args[0] for call in before.await_args_list] == [
        "qdrant",
        "feishu",
        "feishu",
    ]
    delete_replaced.assert_not_awaited()
    terminal_statuses = {
        call.args[1]
        for call in ctx.db_manager.update_status.await_args_list
        if len(call.args) > 1
    }
    assert "waiting_approval" not in terminal_statuses
    assert "notified_readonly" not in terminal_statuses


@pytest.mark.asyncio
async def test_guarded_model_exception_stops_before_state_projection() -> None:
    ref = ContentRef(
        account_id=8,
        object_id="00000000-0000-4000-8000-000000000099",
        key_version="v1",
        sha256="9" * 64,
    )

    async def failing_stream(_state, *, config):
        raise RuntimeError("model unavailable")
        if False:
            yield config

    ctx = SimpleNamespace(
        db_manager=SimpleNamespace(
            get_content_ref=AsyncMock(return_value=ref),
            update_status=AsyncMock(),
            load_draft=AsyncMock(),
        ),
        content_store=SimpleNamespace(
            load_email=AsyncMock(
                return_value={
                    "id": "message-1",
                    "subject": "subject",
                    "sender": "sender@example.test",
                }
            )
        ),
        graph=SimpleNamespace(
            astream=failing_stream,
            aget_state=AsyncMock(),
            aupdate_state=AsyncMock(),
        ),
    )
    before = AsyncMock(return_value=None)
    boundary = ExternalEffectBoundary(_scope("message-1"), before)

    with patch(
        "src.exchange_service.get_settings",
        return_value=SimpleNamespace(EXCHANGE_ACCOUNT_ID=8),
    ):
        with pytest.raises(GuardedExternalEffectFailed):
            await _run_ai_pipeline(
                "message-1",
                ctx,
                {"configurable": {"thread_id": "message-1"}},
                _state_lock_held=True,
                _effect_boundary=boundary,
            )

    assert [call.args[0] for call in before.await_args_list] == [
        "content",
        "model",
    ]
    ctx.graph.aget_state.assert_not_awaited()


@pytest.mark.parametrize("outbound", ["pdf", "card"])
@pytest.mark.asyncio
async def test_unguarded_dispatch_preserves_legacy_remote_exception(
    outbound: str,
) -> None:
    ctx = _guarded_dispatch_ctx()
    failure = RuntimeError(f"legacy {outbound} failure")
    pdf_mock = AsyncMock(
        return_value={"file_token": "pdf", "url": "https://pdf"},
        side_effect=failure if outbound == "pdf" else None,
    )
    card_mock = MagicMock(
        return_value=True,
        side_effect=failure if outbound == "card" else None,
    )

    with (
        patch(
            "src.exchange_service.lark_app.generate_and_upload_pdf",
            new=pdf_mock,
        ),
        patch(
            "src.exchange_service._stage_notification_pdf",
            new=AsyncMock(
                return_value=NotificationPdfStage(
                    ready=True,
                    old_token=None,
                    new_token="pdf",
                    url="https://pdf",
                )
            ),
        ),
        patch(
            "src.exchange_service.lark_app.send_approval_card",
            new=card_mock,
        ),
        patch(
            "src.exchange_service._delete_replaced_pdf",
            new=AsyncMock(),
        ) as delete_replaced,
    ):
        with pytest.raises(RuntimeError) as caught:
            await _dispatch_notification(
                "message-1",
                _guarded_pipeline_result(need_reply=True),
                ctx,
                {},
            )

    assert caught.value is failure
    delete_replaced.assert_not_awaited()
    if outbound == "pdf":
        card_mock.assert_not_called()
