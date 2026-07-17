"""
C1: Two-phase mark_as_read tests.

Verify that an email is only marked as read on Exchange AFTER the user-facing
delivery (Lark card / explicit skip) succeeds. Card delivery failure must
leave the email unread on the server so SelfHealer / human can retry.
"""

import asyncio
import threading
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.domain.email_state import ProcessingOutcome
from src.domain.errors import DatabaseOperationError
from src.exchange_service import (
    _checkpoint_ai_path_resources,
    _cleanup_graph_drive_files,
    _dispatch_notification,
    _retain_cleanup_token,
    _run_ai_path,
    _run_ai_pipeline,
    process_and_archive_email,
)
from src.storage import ContentRef
from src.utils import lark_app, lark_pdf_flow
from src.utils.lark_pdf_flow import PdfFlowOutcome


@pytest.fixture
def ctx():
    from src.exchange_service import get_settings

    c = MagicMock()
    c.db_manager = AsyncMock()
    c.email_processor = MagicMock()
    c.graph = AsyncMock()
    c.exchange_client = AsyncMock()
    ref = ContentRef(
        account_id=get_settings().EXCHANGE_ACCOUNT_ID,
        object_id="00000000-0000-4000-8000-000000000057",
        key_version="v1",
        sha256="5" * 64,
    )
    c._stored_email = {}
    c.content_store = AsyncMock()
    c.content_store.put_email.side_effect = (
        lambda _account, _email_id, email: c._stored_email.update(dict(email)) or ref
    )
    c.content_store.load_email.side_effect = lambda _ref: dict(c._stored_email)
    c.db_manager.get_content_ref.return_value = ref
    c.db_manager.load_draft.return_value = "d"
    return c


def _pipeline_result(need_reply=True, priority="P1", intent="审批"):
    return {
        "classification": {"need_reply": need_reply, "priority": priority, "intent": intent},
        "draft": "draft body",
        "context": [],
        "email": {"id": "msg-c1", "subject": "s"},
        "routing_log": [],
        "active_skills": [],
    }


@pytest.mark.asyncio
async def test_concurrent_cleanup_handle_retention_never_loses_a_token():
    values = {"attachment_tokens": [], "pdf_token": None}

    async def get_state(_config):
        await asyncio.sleep(0.01)
        snapshot = MagicMock()
        snapshot.values = {
            "attachment_tokens": list(values["attachment_tokens"]),
            "pdf_token": values["pdf_token"],
        }
        return snapshot

    async def update_state(_config, delta):
        await asyncio.sleep(0)
        values.update(delta)

    context = MagicMock()
    context.graph = MagicMock()
    context.graph.aget_state = AsyncMock(side_effect=get_state)
    context.graph.aupdate_state = AsyncMock(side_effect=update_state)

    results = await asyncio.gather(
        _retain_cleanup_token("mail-concurrent", context, "TOKEN-A"),
        _retain_cleanup_token("mail-concurrent", context, "TOKEN-B"),
    )

    assert results == [True, True]
    assert set(values["attachment_tokens"]) == {"TOKEN-A", "TOKEN-B"}


@pytest.mark.asyncio
async def test_cleanup_and_concurrent_retain_share_the_same_email_lock():
    values = {
        "attachment_tokens": ["OLD"],
        "pdf_token": None,
    }
    delete_started = threading.Event()
    allow_delete = threading.Event()

    async def get_state(_config):
        snapshot = MagicMock()
        snapshot.values = {
            "attachment_tokens": list(values["attachment_tokens"]),
            "pdf_token": values["pdf_token"],
        }
        snapshot.next = ()
        return snapshot

    async def update_state(_config, delta, **_kwargs):
        values.update(delta)

    def delete_file(_token):
        delete_started.set()
        assert allow_delete.wait(timeout=2)
        return True

    context = MagicMock()
    context.graph = MagicMock()
    context.graph.aget_state = AsyncMock(side_effect=get_state)
    context.graph.aupdate_state = AsyncMock(side_effect=update_state)

    with patch(
        "src.exchange_service.lark_app.delete_file_from_drive",
        side_effect=delete_file,
    ):
        cleanup_task = asyncio.create_task(
            _cleanup_graph_drive_files(
                "mail-concurrent-cleanup",
                context,
                fallback_attachment_tokens=[],
            )
        )
        assert await asyncio.to_thread(delete_started.wait, 1)

        retain_task = asyncio.create_task(
            _retain_cleanup_token(
                "mail-concurrent-cleanup",
                context,
                "NEW",
            )
        )
        completed, _pending = await asyncio.wait({retain_task}, timeout=0.05)
        allow_delete.set()
        await asyncio.gather(cleanup_task, retain_task)

    assert completed == set()
    assert values["attachment_tokens"] == ["NEW"]


@pytest.mark.asyncio
async def test_seed_checkpoint_and_concurrent_retain_share_the_same_email_lock():
    values = {
        "attachment_tokens": [],
        "pdf_token": None,
    }
    seed_read_started = asyncio.Event()
    allow_seed_read = asyncio.Event()
    reads = 0

    async def get_state(_config):
        nonlocal reads
        reads += 1
        snapshot = MagicMock()
        snapshot.values = dict(values)
        snapshot.values["attachment_tokens"] = list(values["attachment_tokens"])
        if reads == 1:
            seed_read_started.set()
            await allow_seed_read.wait()
        return snapshot

    async def update_state(_config, delta, **_kwargs):
        values.update(delta)

    context = MagicMock()
    context.graph = MagicMock()
    context.graph.aget_state = AsyncMock(side_effect=get_state)
    context.graph.aupdate_state = AsyncMock(side_effect=update_state)
    ref = ContentRef(
        account_id=8,
        object_id="00000000-0000-4000-8000-000000000058",
        key_version="v1",
        sha256="6" * 64,
    )

    seed_task = asyncio.create_task(
        _checkpoint_ai_path_resources(
            "mail-concurrent-seed",
            {"id": "mail-concurrent-seed"},
            ref,
            context,
            {"configurable": {"thread_id": "mail-concurrent-seed"}},
            attachment_tokens=[],
            pdf_token=None,
        )
    )
    await seed_read_started.wait()
    retain_task = asyncio.create_task(
        _retain_cleanup_token(
            "mail-concurrent-seed",
            context,
            "NEW",
        )
    )
    completed, _pending = await asyncio.wait({retain_task}, timeout=0.05)
    allow_seed_read.set()
    await asyncio.gather(seed_task, retain_task)

    assert completed == set()
    assert "NEW" in values["attachment_tokens"]


@pytest.mark.asyncio
async def test_pdf_flow_and_exchange_updates_share_the_same_email_lock():
    values = {"attachment_tokens": [], "pdf_token": None}
    flow_started = asyncio.Event()
    allow_flow = asyncio.Event()

    async def get_state(_config):
        snapshot = MagicMock()
        snapshot.values = {
            "attachment_tokens": list(values["attachment_tokens"]),
            "pdf_token": values["pdf_token"],
        }
        return snapshot

    async def update_state(_config, delta):
        values.update(delta)

    async def blocked_pdf_flow(*_args, **_kwargs):
        flow_started.set()
        await allow_flow.wait()
        return None

    graph = MagicMock()
    graph.aget_state = AsyncMock(side_effect=get_state)
    graph.aupdate_state = AsyncMock(side_effect=update_state)
    context = MagicMock(graph=graph)

    with patch.object(
        lark_pdf_flow,
        "_process_pdf_generation_and_reply_locked",
        side_effect=blocked_pdf_flow,
    ):
        flow_task = asyncio.create_task(
            lark_pdf_flow.process_pdf_generation_and_reply(
                "mail-shared-pdf-lock",
                MagicMock(),
                "message-id",
                graph=graph,
                dependencies=MagicMock(),
                lark_api_client=MagicMock(),
                upload_fn=MagicMock(),
                delete_fn=MagicMock(),
            )
        )
        await flow_started.wait()
        retain_task = asyncio.create_task(
            _retain_cleanup_token(
                "mail-shared-pdf-lock",
                context,
                "NEW",
            )
        )
        completed, _pending = await asyncio.wait({retain_task}, timeout=0.05)
        allow_flow.set()
        await asyncio.gather(flow_task, retain_task)

    assert completed == set()
    assert values["attachment_tokens"] == ["NEW"]


@pytest.mark.asyncio
async def test_action_cleanup_and_exchange_updates_share_the_same_email_lock():
    values = {"attachment_tokens": ["OLD"], "pdf_token": None}
    delete_started = threading.Event()
    allow_delete = threading.Event()

    async def get_state(_config):
        snapshot = MagicMock()
        snapshot.values = {
            "attachment_tokens": list(values["attachment_tokens"]),
            "pdf_token": values["pdf_token"],
        }
        return snapshot

    async def update_state(_config, delta):
        values.update(delta)

    def delete_file(_token):
        delete_started.set()
        assert allow_delete.wait(timeout=2)
        return True

    graph = MagicMock()
    graph.aget_state = AsyncMock(side_effect=get_state)
    graph.aupdate_state = AsyncMock(side_effect=update_state)
    context = MagicMock(graph=graph)
    state = MagicMock()
    state.values = {
        "attachment_tokens": ["OLD"],
        "pdf_token": None,
    }

    with patch.object(lark_app, "graph", graph), patch.object(
        lark_app,
        "delete_file_from_drive",
        side_effect=delete_file,
    ):
        cleanup_task = asyncio.create_task(
            lark_app._cleanup_action_drive_tokens(
                "mail-shared-action-lock",
                state,
            )
        )
        assert await asyncio.to_thread(delete_started.wait, 1)
        retain_task = asyncio.create_task(
            _retain_cleanup_token(
                "mail-shared-action-lock",
                context,
                "NEW",
            )
        )
        completed, _pending = await asyncio.wait({retain_task}, timeout=0.05)
        allow_delete.set()
        await asyncio.gather(cleanup_task, retain_task)

    assert completed == set()
    assert values["attachment_tokens"] == ["NEW"]


@pytest.mark.asyncio
async def test_action_cleanup_reconciles_stale_targets_before_remote_delete():
    values = {
        "attachment_tokens": ["NEW"],
        "pdf_token": "OLD",
    }

    async def get_state(_config):
        snapshot = MagicMock()
        snapshot.values = {
            "attachment_tokens": list(values["attachment_tokens"]),
            "pdf_token": values["pdf_token"],
        }
        return snapshot

    async def update_state(_config, delta):
        values.update(delta)

    graph = MagicMock()
    graph.aget_state = AsyncMock(side_effect=get_state)
    graph.aupdate_state = AsyncMock(side_effect=update_state)
    stale_state = MagicMock()
    stale_state.values = {
        "attachment_tokens": ["OLD"],
        "pdf_token": "NEW",
    }
    delete_file = MagicMock(return_value=True)

    with patch.object(lark_app, "graph", graph), patch.object(
        lark_app,
        "delete_file_from_drive",
        delete_file,
    ):
        await lark_app._cleanup_action_drive_tokens(
            "mail-stale-action-cleanup",
            stale_state,
        )

    assert [call.args[0] for call in delete_file.call_args_list] == ["NEW"]
    assert values == {
        "attachment_tokens": [],
        "pdf_token": "OLD",
    }


@pytest.mark.asyncio
async def test_graph_resume_holds_resource_lock_until_action_cleanup_finishes():
    email_id = "mail-resume-cleanup-lock"
    values = {"attachment_tokens": [], "pdf_token": None}
    resume_started = asyncio.Event()
    allow_resume = asyncio.Event()

    async def invoke(_input, *, config):
        resume_started.set()
        await allow_resume.wait()
        return config

    async def get_state(_config):
        snapshot = MagicMock()
        snapshot.values = {
            "attachment_tokens": list(values["attachment_tokens"]),
            "pdf_token": values["pdf_token"],
        }
        return snapshot

    async def update_state(_config, delta):
        values.update(delta)

    graph = MagicMock()
    graph.ainvoke = AsyncMock(side_effect=invoke)
    graph.aget_state = AsyncMock(side_effect=get_state)
    graph.aupdate_state = AsyncMock(side_effect=update_state)
    context = MagicMock(graph=graph)
    initial_state = MagicMock()
    initial_state.values = dict(values)
    config = {"configurable": {"thread_id": email_id}}

    with patch.object(lark_app, "graph", graph):
        resume_task = asyncio.create_task(
            lark_app._resume_graph_then_cleanup(
                email_id,
                initial_state,
                config,
            )
        )
        await resume_started.wait()
        retain_task = asyncio.create_task(
            _retain_cleanup_token(email_id, context, "NEW")
        )
        completed, _pending = await asyncio.wait({retain_task}, timeout=0.05)
        allow_resume.set()
        await asyncio.gather(resume_task, retain_task)

    assert completed == set()
    assert values["attachment_tokens"] == ["NEW"]


@pytest.mark.asyncio
async def test_ai_graph_seed_and_resource_updates_share_the_same_email_lock():
    email_id = "mail-shared-ai-lock"
    values = {
        "email_id": email_id,
        "classification": {"need_reply": False},
        "draft_id": None,
        "draft_to": [],
        "draft_cc": [],
        "context_summaries": [],
        "routing_log": [],
        "active_skills": [],
        "attachment_tokens": [],
        "pdf_token": None,
    }
    pipeline_started = asyncio.Event()
    allow_pipeline = asyncio.Event()

    async def stream(_initial_state, *, config):
        pipeline_started.set()
        await allow_pipeline.wait()
        if False:
            yield config

    async def get_state(_config):
        snapshot = MagicMock()
        snapshot.values = dict(values)
        snapshot.values["attachment_tokens"] = list(values["attachment_tokens"])
        return snapshot

    async def update_state(_config, delta):
        values.update(delta)

    graph = MagicMock()
    graph.astream = stream
    graph.aget_state = AsyncMock(side_effect=get_state)
    graph.aupdate_state = AsyncMock(side_effect=update_state)
    ref = ContentRef(
        account_id=8,
        object_id="00000000-0000-4000-8000-000000000059",
        key_version="v1",
        sha256="7" * 64,
    )
    context = MagicMock()
    context.graph = graph
    context.db_manager.get_content_ref = AsyncMock(return_value=ref)
    context.db_manager.update_status = AsyncMock()
    context.db_manager.load_draft = AsyncMock()
    context.content_store.load_email = AsyncMock(return_value={"id": email_id})

    with patch(
        "src.exchange_service.get_settings",
        return_value=MagicMock(EXCHANGE_ACCOUNT_ID=8),
    ):
        pipeline_task = asyncio.create_task(
            _run_ai_pipeline(
                email_id,
                context,
                {"configurable": {"thread_id": email_id}},
            )
        )
        await pipeline_started.wait()
        retain_task = asyncio.create_task(
            _retain_cleanup_token(email_id, context, "NEW")
        )
        completed, _pending = await asyncio.wait({retain_task}, timeout=0.05)
        allow_pipeline.set()
        pipeline_result, retained = await asyncio.gather(
            pipeline_task,
            retain_task,
        )

    assert completed == set()
    assert retained is True
    assert pipeline_result is not None
    assert values["attachment_tokens"] == ["NEW"]


@pytest.mark.asyncio
async def test_dispatch_returns_delivered_true_when_card_succeeds(ctx):
    with patch("src.exchange_service.lark_app.send_approval_card", return_value=True), \
         patch("src.exchange_service.lark_app.generate_and_upload_pdf",
               new=AsyncMock(return_value=None)):
        result = await _dispatch_notification(
            "msg-c1", _pipeline_result(need_reply=True), ctx, {}
        )
    assert result == {"delivered": True, "kind": "approval"}
    ctx.db_manager.update_status.assert_any_call("msg-c1", "waiting_approval")


@pytest.mark.asyncio
async def test_dispatch_returns_delivered_false_on_card_failure(ctx):
    with patch("src.exchange_service.lark_app.send_approval_card", return_value=False), \
         patch("src.exchange_service.lark_app.generate_and_upload_pdf",
               new=AsyncMock(return_value=None)):
        result = await _dispatch_notification(
            "msg-c1", _pipeline_result(need_reply=True), ctx, {}
        )
    assert result == {"delivered": False, "kind": "approval"}
    # status should be 'delivery_failed' instead of 'waiting_approval'
    failed_calls = [
        c for c in ctx.db_manager.update_status.call_args_list
        if "delivery_failed" in c.args
    ]
    assert failed_calls, "Expected status update to delivery_failed"


@pytest.mark.asyncio
async def test_dispatch_deletes_new_pdf_when_token_cannot_enter_graph(ctx):
    state = MagicMock()
    state.values = {
        "email_id": "msg-c1",
        "pdf_token": None,
        "attachment_tokens": [],
    }
    ctx.graph.aget_state.return_value = state
    ctx.graph.aupdate_state.side_effect = RuntimeError("checkpoint write failed")

    with patch(
        "src.exchange_service.lark_app.generate_and_upload_pdf",
        new=AsyncMock(return_value={"url": "URL", "file_token": "NEW-PDF"}),
    ), patch(
        "src.exchange_service.lark_app.delete_file_from_drive",
        return_value=True,
    ) as delete, patch(
        "src.exchange_service.lark_app.send_approval_card",
        return_value=True,
    ) as send:
        result = await _dispatch_notification(
            "msg-c1", _pipeline_result(need_reply=True), ctx, {}
        )

    assert result == {"delivered": True, "kind": "approval"}
    delete.assert_called_once_with("NEW-PDF")
    assert send.call_args.kwargs["pdf_url"] is None


@pytest.mark.asyncio
async def test_dispatch_fails_closed_when_pdf_write_and_cleanup_are_untracked(ctx):
    state = MagicMock()
    state.values = {
        "email_id": "msg-c1",
        "pdf_token": None,
        "attachment_tokens": [],
    }
    ctx.graph.aget_state.return_value = state
    ctx.graph.aupdate_state.side_effect = RuntimeError("checkpoint write failed")

    with patch(
        "src.exchange_service.lark_app.generate_and_upload_pdf",
        new=AsyncMock(return_value={"url": "URL", "file_token": "NEW-PDF"}),
    ), patch(
        "src.exchange_service.lark_app.delete_file_from_drive",
        return_value=False,
    ), patch(
        "src.exchange_service.lark_app.send_approval_card",
        return_value=True,
    ) as send:
        result = await _dispatch_notification(
            "msg-c1", _pipeline_result(need_reply=True), ctx, {}
        )

    assert result == {"delivered": False, "kind": "approval"}
    send.assert_not_called()
    ctx.db_manager.update_status.assert_any_call(
        "msg-c1",
        "delivery_failed",
        error_message="notification_pdf_stage_failed",
    )


@pytest.mark.asyncio
async def test_dispatch_reconciles_committed_pdf_when_write_ack_is_lost(ctx):
    old_state = MagicMock()
    old_state.values = {
        "email_id": "msg-c1",
        "pdf_token": "OLD-PDF",
        "attachment_tokens": [],
    }
    committed_state = MagicMock()
    committed_state.values = {
        **old_state.values,
        "pdf_token": "NEW-PDF",
        "attachment_tokens": ["OLD-PDF"],
    }
    ctx.graph.aget_state.side_effect = [
        old_state,
        committed_state,
        committed_state,
        committed_state,
    ]
    ctx.graph.aupdate_state.side_effect = RuntimeError("ack lost")

    with patch(
        "src.exchange_service.lark_app.generate_and_upload_pdf",
        new=AsyncMock(return_value={"url": "URL", "file_token": "NEW-PDF"}),
    ), patch(
        "src.exchange_service.lark_app.delete_file_from_drive",
        return_value=True,
    ) as delete, patch(
        "src.exchange_service.lark_app.send_approval_card",
        return_value=True,
    ) as send:
        result = await _dispatch_notification(
            "msg-c1", _pipeline_result(need_reply=True), ctx, {}
        )

    assert result == {"delivered": True, "kind": "approval"}
    assert send.call_args.kwargs["pdf_url"] == "URL"
    delete.assert_called_once_with("OLD-PDF")


@pytest.mark.asyncio
async def test_dispatch_cleans_candidate_when_concurrent_pdf_wins(ctx):
    old_state = MagicMock()
    old_state.values = {
        "email_id": "msg-c1",
        "pdf_token": "OLD-PDF",
        "attachment_tokens": [],
    }
    concurrent_state = MagicMock()
    concurrent_state.values = {
        **old_state.values,
        "pdf_token": "OTHER-PDF",
    }
    ctx.graph.aget_state.side_effect = [old_state, concurrent_state]
    ctx.graph.aupdate_state.side_effect = RuntimeError("write raced")

    with patch(
        "src.exchange_service.lark_app.generate_and_upload_pdf",
        new=AsyncMock(return_value={"url": "URL", "file_token": "NEW-PDF"}),
    ), patch(
        "src.exchange_service.lark_app.delete_file_from_drive",
        return_value=True,
    ) as delete, patch(
        "src.exchange_service.lark_app.send_approval_card",
        return_value=True,
    ) as send:
        result = await _dispatch_notification(
            "msg-c1", _pipeline_result(need_reply=True), ctx, {}
        )

    assert result == {"delivered": True, "kind": "approval"}
    delete.assert_called_once_with("NEW-PDF")
    assert send.call_args.kwargs["pdf_url"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("delete_new", "expected_delivered"),
    [(True, True), (False, False)],
    ids=["new-cleaned", "new-untracked"],
)
async def test_dispatch_does_not_replace_pdf_without_old_handle_capacity(
    ctx,
    delete_new,
    expected_delivered,
):
    state = MagicMock()
    state.values = {
        "email_id": "msg-c1",
        "pdf_token": "OLD-PDF",
        "attachment_tokens": [f"KEEP-{index}" for index in range(32)],
    }
    ctx.graph.aget_state.return_value = state

    with patch(
        "src.exchange_service.lark_app.generate_and_upload_pdf",
        new=AsyncMock(return_value={"url": "URL", "file_token": "NEW-PDF"}),
    ), patch(
        "src.exchange_service.lark_app.delete_file_from_drive",
        return_value=delete_new,
    ) as delete, patch(
        "src.exchange_service.lark_app.send_approval_card",
        return_value=True,
    ) as send:
        result = await _dispatch_notification(
            "msg-c1", _pipeline_result(need_reply=True), ctx, {}
        )

    assert result == {"delivered": expected_delivered, "kind": "approval"}
    delete.assert_called_once_with("NEW-PDF")
    if expected_delivered:
        assert send.call_args.kwargs["pdf_url"] is None
    else:
        send.assert_not_called()


@pytest.mark.asyncio
async def test_dispatch_tracks_pdf_cleanup_outcome_before_sending_without_link(ctx):
    state = MagicMock()
    state.values = {
        "email_id": "msg-c1",
        "pdf_token": None,
        "attachment_tokens": [],
    }

    async def update_state(_config, delta, **_kwargs):
        state.values.update(delta)

    ctx.graph.aget_state.return_value = state
    ctx.graph.aupdate_state.side_effect = update_state
    with patch(
        "src.exchange_service.lark_app.generate_and_upload_pdf",
        new=AsyncMock(
            return_value=PdfFlowOutcome(
                status="upload_invalid_cleanup_required",
                retryable=True,
                cleanup_tokens=("UNTRACKED-PDF",),
            )
        ),
    ), patch(
        "src.exchange_service.lark_app.send_approval_card",
        return_value=True,
    ) as send:
        result = await _dispatch_notification(
            "msg-c1", _pipeline_result(need_reply=True), ctx, {}
        )

    assert result == {"delivered": True, "kind": "approval"}
    assert state.values["attachment_tokens"] == ["UNTRACKED-PDF"]
    assert send.call_args.kwargs["pdf_url"] is None


@pytest.mark.asyncio
async def test_dispatch_fails_closed_on_unpersistable_pdf_cleanup_outcome(ctx):
    state = MagicMock()
    state.values = {
        "email_id": "msg-c1",
        "pdf_token": None,
        "attachment_tokens": [],
    }
    ctx.graph.aget_state.return_value = state

    with patch(
        "src.exchange_service.lark_app.generate_and_upload_pdf",
        new=AsyncMock(
            return_value=PdfFlowOutcome(
                status="upload_invalid_cleanup_required",
                retryable=True,
                cleanup_tokens=("x" * 513,),
            )
        ),
    ), patch(
        "src.exchange_service.lark_app.send_approval_card",
        return_value=True,
    ) as send:
        result = await _dispatch_notification(
            "msg-c1", _pipeline_result(need_reply=True), ctx, {}
        )

    assert result == {"delivered": False, "kind": "approval"}
    send.assert_not_called()


@pytest.mark.asyncio
async def test_dispatch_retains_old_pdf_handle_when_remote_delete_fails(ctx):
    state = MagicMock()
    state.values = {
        "email_id": "msg-c1",
        "pdf_token": "OLD-PDF",
        "attachment_tokens": [],
    }
    ctx.graph.aget_state.return_value = state

    with patch(
        "src.exchange_service.lark_app.generate_and_upload_pdf",
        new=AsyncMock(return_value={"url": "URL", "file_token": "NEW-PDF"}),
    ), patch(
        "src.exchange_service.lark_app.delete_file_from_drive",
        return_value=False,
    ), patch(
        "src.exchange_service.lark_app.send_approval_card",
        return_value=True,
    ):
        result = await _dispatch_notification(
            "msg-c1", _pipeline_result(need_reply=True), ctx, {}
        )

    assert result == {"delivered": True, "kind": "approval"}
    assert any(
        call.args[1].get("attachment_tokens") == ["OLD-PDF"]
        for call in ctx.graph.aupdate_state.await_args_list
    )


@pytest.mark.asyncio
async def test_failed_replacement_card_restores_old_pdf_without_deleting_it(ctx):
    state = MagicMock()
    state.values = {
        "email_id": "msg-c1",
        "pdf_token": "OLD-PDF",
        "attachment_tokens": [],
    }

    async def update_state(_config, delta, **_kwargs):
        state.values.update(delta)

    ctx.graph.aget_state.return_value = state
    ctx.graph.aupdate_state.side_effect = update_state

    with patch(
        "src.exchange_service._upload_attachments_to_lark",
        new=AsyncMock(
            return_value=MagicMock(tokens=(), links=())
        ),
    ), patch(
        "src.exchange_service._ingest_to_qdrant",
        new_callable=AsyncMock,
    ), patch(
        "src.exchange_service._run_ai_pipeline",
        new=AsyncMock(return_value=_pipeline_result(need_reply=True)),
    ), patch(
        "src.exchange_service.lark_app.generate_and_upload_pdf",
        new=AsyncMock(return_value={"url": "URL", "file_token": "NEW-PDF"}),
    ), patch(
        "src.exchange_service.lark_app.send_approval_card",
        return_value=False,
    ), patch(
        "src.exchange_service.lark_app.delete_file_from_drive",
        return_value=True,
    ) as delete:
        outcome = await _run_ai_path(
            "msg-c1",
            {"id": "msg-c1"},
            ctx,
            {"configurable": {"thread_id": "msg-c1"}},
        )

    assert outcome is ProcessingOutcome.FAILED
    assert {call.args[0] for call in delete.call_args_list} == {"NEW-PDF"}
    assert state.values["pdf_token"] == "OLD-PDF"
    assert "OLD-PDF" not in state.values["attachment_tokens"]
    ctx.exchange_client.mark_as_read.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("need_reply", "failure_status", "send_function", "failure_kind"),
    [
        (True, "waiting_approval", "send_approval_card", "database"),
        (False, "notified_readonly", "send_read_only_card", "database"),
        (True, "waiting_approval", "send_approval_card", "cancelled"),
        (False, "notified_readonly", "send_read_only_card", "cancelled"),
    ],
)
async def test_delivered_card_status_failure_preserves_all_card_resources(
    ctx,
    need_reply,
    failure_status,
    send_function,
    failure_kind,
):
    if failure_kind == "cancelled":
        failure = asyncio.CancelledError()
    else:
        failure = DatabaseOperationError(
            operation="update_status",
            retryable=True,
            message="notification status write failed",
        )

    async def update_status(_email_id, status, **_kwargs):
        if status == failure_status:
            raise failure

    state = MagicMock()
    state.values = {
        "email_id": "msg-c1",
        "pdf_token": "NEW-PDF",
        "attachment_tokens": ["NEW-ATTACHMENT"],
    }
    ctx.graph.aget_state.return_value = state
    ctx.db_manager.update_status.side_effect = update_status

    with patch(
        "src.exchange_service._snapshot_cleanup_handles",
        new=AsyncMock(
            return_value=MagicMock(attachment_tokens=(), pdf_token=None)
        ),
    ), patch(
        "src.exchange_service._checkpoint_ai_path_resources",
        new=AsyncMock(
            return_value=MagicMock(attachment_tokens=(), pdf_token=None)
        ),
    ), patch(
        "src.exchange_service._upload_attachments_to_lark",
        new=AsyncMock(
            return_value=MagicMock(tokens=("NEW-ATTACHMENT",), links=())
        ),
    ), patch(
        "src.exchange_service._ingest_to_qdrant",
        new_callable=AsyncMock,
    ), patch(
        "src.exchange_service._run_ai_pipeline",
        new=AsyncMock(return_value=_pipeline_result(need_reply=need_reply)),
    ), patch(
        "src.exchange_service.lark_app.generate_and_upload_pdf",
        new=AsyncMock(return_value={"url": "URL", "file_token": "NEW-PDF"}),
    ), patch(
        f"src.exchange_service.lark_app.{send_function}",
        return_value=True,
    ), patch(
        "src.exchange_service.lark_app.delete_file_from_drive",
        return_value=True,
    ) as delete, pytest.raises(type(failure)) as caught:
        await _run_ai_path(
            "msg-c1",
            {"id": "msg-c1"},
            ctx,
            {"configurable": {"thread_id": "msg-c1"}},
        )

    assert caught.value is failure
    delete.assert_not_called()
    ctx.exchange_client.mark_as_read.assert_not_awaited()


@pytest.mark.asyncio
async def test_mark_read_cancellation_after_delivered_card_preserves_resources(ctx):
    baseline = MagicMock(attachment_tokens=(), pdf_token=None)
    state = MagicMock()
    state.values = {
        "email_id": "msg-c1",
        "pdf_token": "NEW-PDF",
        "attachment_tokens": ["NEW-ATTACHMENT"],
    }
    ctx.graph.aget_state.return_value = state
    cancellation = asyncio.CancelledError()

    with patch(
        "src.exchange_service._snapshot_cleanup_handles",
        new=AsyncMock(return_value=baseline),
    ), patch(
        "src.exchange_service._checkpoint_ai_path_resources",
        new=AsyncMock(return_value=baseline),
    ), patch(
        "src.exchange_service._upload_attachments_to_lark",
        new=AsyncMock(
            return_value=MagicMock(tokens=("NEW-ATTACHMENT",), links=())
        ),
    ), patch(
        "src.exchange_service._ingest_to_qdrant",
        new_callable=AsyncMock,
    ), patch(
        "src.exchange_service._run_ai_pipeline",
        new=AsyncMock(return_value=_pipeline_result(need_reply=True)),
    ), patch(
        "src.exchange_service._dispatch_notification",
        new=AsyncMock(return_value={"delivered": True, "kind": "approval"}),
    ), patch(
        "src.exchange_service._mark_email_read",
        new=AsyncMock(side_effect=cancellation),
    ), patch(
        "src.exchange_service.lark_app.delete_file_from_drive",
        return_value=True,
    ) as delete, pytest.raises(asyncio.CancelledError) as caught:
        await _run_ai_path(
            "msg-c1",
            {"id": "msg-c1"},
            ctx,
            {"configurable": {"thread_id": "msg-c1"}},
        )

    assert caught.value is cancellation
    delete.assert_not_called()


@pytest.mark.asyncio
async def test_dispatch_read_only_failure_marks_delivery_failed(ctx):
    with patch("src.exchange_service.lark_app.send_read_only_card", return_value=False), \
         patch("src.exchange_service.lark_app.generate_and_upload_pdf",
               new=AsyncMock(return_value=None)):
        result = await _dispatch_notification(
            "msg-c1",
            _pipeline_result(need_reply=False, priority="P1", intent="通知"),
            ctx,
            {},
        )
    assert result == {"delivered": False, "kind": "read_only"}


@pytest.mark.asyncio
async def test_dispatch_skipped_path_treats_as_delivered(ctx):
    """P3/non-notification mails are intentionally not surfaced - count as delivered."""
    result = await _dispatch_notification(
        "msg-c1",
        _pipeline_result(need_reply=False, priority="P3", intent="垃圾邮件"),
        ctx,
        {},
    )
    assert result == {"delivered": True, "kind": "skipped"}
    ctx.db_manager.update_status.assert_any_call("msg-c1", "skipped")


@pytest.mark.asyncio
async def test_process_email_skips_mark_read_when_dispatch_fails(ctx):
    """Card delivery failure must leave the email unread on Exchange."""
    ctx.db_manager.log_initial_email.return_value = True

    async def mock_astream(*a, **k):
        yield {"categorizer": {"classification": {"need_reply": True, "priority": "P1"}}}
        yield {"drafter": {"draft_id": "msg-fail"}}
    ctx.graph.astream = mock_astream

    final_state = MagicMock()
    final_state.values = {
        "email_id": "msg-fail",
        "classification": {"need_reply": True, "priority": "P1", "intent": "审批"},
        "draft_id": "msg-fail",
        "context_summaries": [],
        "routing_log": [],
        "active_skills": [],
        "draft_to": [],
        "draft_cc": [],
    }
    ctx.graph.aget_state.return_value = final_state

    with patch(
        "src.exchange_service._checkpoint_ai_path_resources",
        new=AsyncMock(
            return_value=MagicMock(attachment_tokens=(), pdf_token=None)
        ),
    ), patch("src.exchange_service.lark_app.send_approval_card", return_value=False), \
         patch("src.exchange_service.lark_app.generate_and_upload_pdf",
               new=AsyncMock(return_value=None)):
        await process_and_archive_email({"id": "msg-fail"}, ctx)

    ctx.exchange_client.mark_as_read.assert_not_called()


@pytest.mark.asyncio
async def test_process_email_marks_read_only_after_successful_dispatch(ctx):
    ctx.db_manager.log_initial_email.return_value = True

    async def mock_astream(*a, **k):
        yield {"categorizer": {"classification": {"need_reply": True, "priority": "P1"}}}
        yield {"drafter": {"draft_id": "msg-ok"}}
    ctx.graph.astream = mock_astream

    final_state = MagicMock()
    final_state.values = {
        "email_id": "msg-ok",
        "classification": {"need_reply": True, "priority": "P1", "intent": "审批"},
        "draft_id": "msg-ok",
        "context_summaries": [],
        "routing_log": [],
        "active_skills": [],
        "draft_to": [],
        "draft_cc": [],
    }
    ctx.graph.aget_state.return_value = final_state
    ctx.exchange_client.mark_as_read.return_value = True

    with patch(
        "src.exchange_service._checkpoint_ai_path_resources",
        new=AsyncMock(
            return_value=MagicMock(attachment_tokens=(), pdf_token=None)
        ),
    ), patch("src.exchange_service.lark_app.send_approval_card", return_value=True), \
         patch("src.exchange_service.lark_app.generate_and_upload_pdf",
               new=AsyncMock(return_value=None)):
        await process_and_archive_email({"id": "msg-ok"}, ctx)

    ctx.exchange_client.mark_as_read.assert_called_once_with("msg-ok", is_read=True)


@pytest.mark.asyncio
async def test_self_healer_picks_up_delivery_failed():
    """SelfHealer's stuck list query must include delivery_failed status."""
    from src.utils import self_healing
    assert "delivery_failed" in self_healing.STUCK_STATUSES
