from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from src.exchange_service import (
    CleanupHandleSnapshot,
    NotificationPdfStage,
    _cleanup_graph_drive_files,
    _delete_drive_token_or_retain,
    _delete_replaced_pdf,
    _delete_unclaimed_content_candidate,
    _dispatch_notification,
    _ensure_durable_content_ref,
    _ingest_to_qdrant,
    _mark_email_read,
    _stage_notification_pdf,
    _upload_attachments_to_lark,
)
from src.ingestion.processing import (
    ExternalEffectBoundary,
    GuardedExternalEffectFailed,
    LegacyEffectScope,
)
from src.storage import ContentRef


def _scope(email_id: str = "message-1") -> LegacyEffectScope:
    return LegacyEffectScope(
        account_id=8,
        inbox_id=str(uuid4()),
        generation=3,
        fencing_token=7,
        attempts=1,
        email_id=str(uuid4()),
        expected_email_version=4,
        event_dedupe_key="a" * 64,
        external_email_id=email_id,
    )


def _boundary() -> tuple[ExternalEffectBoundary, AsyncMock]:
    port = AsyncMock(return_value=None)
    return ExternalEffectBoundary(_scope(), port), port


def _ref() -> ContentRef:
    return ContentRef(
        account_id=8,
        object_id="00000000-0000-4000-8000-000000000088",
        key_version="v1",
        sha256="8" * 64,
    )


def _pipeline_result(*, need_reply: bool) -> dict[str, object]:
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
        "active_skills": [],
    }


def _dispatch_ctx(*, labels: object = True) -> SimpleNamespace:
    return SimpleNamespace(
        db_manager=SimpleNamespace(update_status=AsyncMock()),
        email_processor=SimpleNamespace(
            update_email_labels=MagicMock(return_value=labels)
        ),
        graph=SimpleNamespace(
            aget_state=AsyncMock(),
            aupdate_state=AsyncMock(),
        ),
    )


@pytest.mark.asyncio
async def test_unguarded_attachment_upload_error_stops_batch_without_raising() -> None:
    with patch(
        "src.exchange_service.lark_app.upload_file_to_drive",
        side_effect=RuntimeError("drive unavailable"),
    ) as upload:
        projection = await _upload_attachments_to_lark(
            {
                "attachments": [
                    {"name": "first.txt", "content": "YQ=="},
                    {"name": "second.txt", "content": "Yg=="},
                ]
            }
        )

    assert projection.tokens == ()
    assert projection.links == ()
    upload.assert_called_once_with("first.txt", b"a", 1)


@pytest.mark.asyncio
async def test_unguarded_qdrant_error_remains_best_effort() -> None:
    ctx = SimpleNamespace(
        email_processor=SimpleNamespace(
            process_email=MagicMock(side_effect=RuntimeError("qdrant unavailable"))
        ),
        db_manager=SimpleNamespace(update_status=AsyncMock()),
    )

    await _ingest_to_qdrant("message-1", {"id": "message-1"}, ctx)

    ctx.db_manager.update_status.assert_not_awaited()


@pytest.mark.asyncio
async def test_successful_reingest_clears_a_prior_safe_error() -> None:
    ctx = SimpleNamespace(
        email_processor=SimpleNamespace(process_email=MagicMock(return_value=True)),
        db_manager=SimpleNamespace(update_status=AsyncMock()),
    )

    await _ingest_to_qdrant("message-1", {"id": "message-1"}, ctx)

    ctx.db_manager.update_status.assert_awaited_once_with(
        "message-1",
        "ingested",
        error_message=None,
    )


@pytest.mark.asyncio
async def test_guarded_pdf_state_write_error_stops_before_reconciliation() -> None:
    boundary, port = _boundary()
    state = SimpleNamespace(
        values={"pdf_token": None, "attachment_tokens": []},
    )
    ctx = SimpleNamespace(
        graph=SimpleNamespace(
            aget_state=AsyncMock(return_value=state),
            aupdate_state=AsyncMock(side_effect=RuntimeError("checkpoint failed")),
        )
    )

    with pytest.raises(GuardedExternalEffectFailed):
        await _stage_notification_pdf(
            "message-1",
            ctx,
            {"file_token": "pdf-token", "url": "https://pdf"},
            _state_lock_held=True,
            _effect_boundary=boundary,
        )

    assert ctx.graph.aget_state.await_count == 1
    port.assert_not_awaited()


@pytest.mark.asyncio
async def test_unguarded_drive_delete_error_is_reconciled_to_a_durable_handle() -> None:
    ctx = SimpleNamespace(graph=SimpleNamespace())
    with (
        patch(
            "src.exchange_service.lark_app.delete_file_from_drive",
            side_effect=RuntimeError("drive unavailable"),
        ),
        patch(
            "src.exchange_service._retain_cleanup_token",
            new=AsyncMock(return_value=True),
        ) as retain,
    ):
        reconciled = await _delete_drive_token_or_retain(
            "message-1",
            ctx,
            "drive-token",
            _state_lock_held=True,
        )

    assert reconciled is True
    retain.assert_awaited_once_with(
        "message-1",
        ctx,
        "drive-token",
        _state_lock_held=True,
    )


@pytest.mark.asyncio
async def test_unguarded_replaced_pdf_delete_error_retains_the_old_handle() -> None:
    ctx = SimpleNamespace(graph=SimpleNamespace())
    with (
        patch(
            "src.exchange_service.lark_app.delete_file_from_drive",
            side_effect=RuntimeError("drive unavailable"),
        ),
        patch(
            "src.exchange_service._retain_cleanup_token",
            new=AsyncMock(return_value=True),
        ) as retain,
    ):
        reconciled = await _delete_replaced_pdf(
            "message-1",
            ctx,
            "old-token",
            "new-token",
        )

    assert reconciled is True
    retain.assert_awaited_once_with("message-1", ctx, "old-token")


@pytest.mark.asyncio
async def test_unguarded_label_error_does_not_block_legacy_card_delivery() -> None:
    ctx = _dispatch_ctx()
    ctx.email_processor.update_email_labels = MagicMock(
        side_effect=RuntimeError("qdrant unavailable")
    )
    with (
        patch(
            "src.exchange_service.lark_app.generate_and_upload_pdf",
            new=AsyncMock(return_value=None),
        ),
        patch(
            "src.exchange_service.lark_app.send_approval_card",
            return_value=True,
        ) as send_card,
    ):
        result = await _dispatch_notification(
            "message-1",
            _pipeline_result(need_reply=True),
            ctx,
            {},
        )

    assert result == {"delivered": True, "kind": "approval"}
    send_card.assert_called_once()


@pytest.mark.asyncio
async def test_unguarded_readonly_pdf_error_preserves_original_exception() -> None:
    ctx = _dispatch_ctx()
    failure = RuntimeError("pdf unavailable")
    with (
        patch(
            "src.exchange_service.lark_app.generate_and_upload_pdf",
            new=AsyncMock(side_effect=failure),
        ),
        patch("src.exchange_service.lark_app.send_read_only_card") as send_card,
    ):
        with pytest.raises(RuntimeError) as caught:
            await _dispatch_notification(
                "message-1",
                _pipeline_result(need_reply=False),
                ctx,
                {},
            )

    assert caught.value is failure
    send_card.assert_not_called()


@pytest.mark.asyncio
async def test_unguarded_readonly_card_error_preserves_original_exception() -> None:
    ctx = _dispatch_ctx()
    failure = RuntimeError("card unavailable")
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
                    new_token="pdf",
                    url="https://pdf",
                )
            ),
        ),
        patch(
            "src.exchange_service.lark_app.send_read_only_card",
            side_effect=failure,
        ),
        patch(
            "src.exchange_service._delete_replaced_pdf",
            new=AsyncMock(),
        ) as delete_replaced,
    ):
        with pytest.raises(RuntimeError) as caught:
            await _dispatch_notification(
                "message-1",
                _pipeline_result(need_reply=False),
                ctx,
                {},
            )

    assert caught.value is failure
    delete_replaced.assert_not_awaited()


@pytest.mark.asyncio
async def test_unguarded_mark_read_error_remains_best_effort() -> None:
    ctx = SimpleNamespace(
        exchange_client=SimpleNamespace(
            mark_as_read=AsyncMock(side_effect=RuntimeError("exchange unavailable"))
        )
    )

    await _mark_email_read("message-1", ctx)

    ctx.exchange_client.mark_as_read.assert_awaited_once_with("message-1", is_read=True)


@pytest.mark.asyncio
async def test_unguarded_cancelled_candidate_cleanup_preserves_legacy_best_effort() -> (
    None
):
    ctx = SimpleNamespace(
        content_store=SimpleNamespace(
            delete=AsyncMock(side_effect=asyncio.CancelledError())
        )
    )

    await _delete_unclaimed_content_candidate(
        _ref(),
        ctx,
        reason="losing-candidate",
    )

    ctx.content_store.delete.assert_awaited_once()


@pytest.mark.asyncio
async def test_unguarded_content_put_error_preserves_original_exception() -> None:
    failure = RuntimeError("content store unavailable")
    ctx = SimpleNamespace(
        content_store=SimpleNamespace(
            put_email=AsyncMock(side_effect=failure),
        ),
        db_manager=SimpleNamespace(
            get_content_ref=AsyncMock(),
            set_content_ref_if_absent=AsyncMock(),
        ),
    )

    with patch(
        "src.exchange_service.get_settings",
        return_value=SimpleNamespace(EXCHANGE_ACCOUNT_ID=8),
    ):
        with pytest.raises(RuntimeError) as caught:
            await _ensure_durable_content_ref(
                "message-1",
                {"id": "message-1"},
                ctx,
                reuse_existing=False,
            )

    assert caught.value is failure
    ctx.db_manager.set_content_ref_if_absent.assert_not_awaited()


@pytest.mark.asyncio
async def test_guarded_cleanup_requires_a_real_persisted_state_snapshot() -> None:
    boundary, port = _boundary()
    ctx = SimpleNamespace(
        graph=SimpleNamespace(
            aget_state=AsyncMock(return_value=None),
            aupdate_state=AsyncMock(),
        )
    )

    with pytest.raises(GuardedExternalEffectFailed):
        await _cleanup_graph_drive_files(
            "message-1",
            ctx,
            fallback_attachment_tokens=[],
            _state_lock_held=True,
            _effect_boundary=boundary,
        )

    port.assert_not_awaited()
    ctx.graph.aupdate_state.assert_not_awaited()


@pytest.mark.asyncio
async def test_unguarded_attachment_cleanup_error_tracks_failed_token() -> None:
    state = SimpleNamespace(
        values={"attachment_tokens": ["attachment-token"], "pdf_token": None},
        next=(),
    )
    ctx = SimpleNamespace(
        graph=SimpleNamespace(
            aget_state=AsyncMock(return_value=state),
            aupdate_state=AsyncMock(),
        )
    )
    confirmed = CleanupHandleSnapshot(attachment_tokens=("attachment-token",))
    with (
        patch(
            "src.exchange_service.lark_app.delete_file_from_drive",
            side_effect=RuntimeError("drive unavailable"),
        ),
        patch(
            "src.exchange_service._snapshot_cleanup_handles",
            new=AsyncMock(return_value=confirmed),
        ),
    ):
        await _cleanup_graph_drive_files(
            "message-1",
            ctx,
            fallback_attachment_tokens=[],
            _state_lock_held=True,
        )

    update = ctx.graph.aupdate_state.await_args.args[1]
    assert update["attachment_tokens"] == ["attachment-token"]


@pytest.mark.asyncio
async def test_unguarded_pdf_cleanup_error_retains_pdf_for_later_retry() -> None:
    state = SimpleNamespace(
        values={"attachment_tokens": [], "pdf_token": "pdf-token"},
        next=(),
    )
    ctx = SimpleNamespace(
        graph=SimpleNamespace(
            aget_state=AsyncMock(return_value=state),
            aupdate_state=AsyncMock(),
        )
    )
    confirmed = CleanupHandleSnapshot(pdf_token="pdf-token")
    with (
        patch(
            "src.exchange_service.lark_app.delete_file_from_drive",
            side_effect=RuntimeError("drive unavailable"),
        ),
        patch(
            "src.exchange_service._snapshot_cleanup_handles",
            new=AsyncMock(return_value=confirmed),
        ),
    ):
        await _cleanup_graph_drive_files(
            "message-1",
            ctx,
            fallback_attachment_tokens=[],
            _state_lock_held=True,
        )

    update = ctx.graph.aupdate_state.await_args.args[1]
    assert update["pdf_token"] == "pdf-token"


def _cleanup_ctx() -> tuple[SimpleNamespace, SimpleNamespace]:
    state = SimpleNamespace(
        values={"attachment_tokens": ["keep-token"], "pdf_token": None},
        next=(),
    )
    ctx = SimpleNamespace(
        graph=SimpleNamespace(
            aget_state=AsyncMock(return_value=state),
            aupdate_state=AsyncMock(),
        )
    )
    return ctx, state


@pytest.mark.asyncio
async def test_unguarded_cleanup_state_update_error_is_best_effort() -> None:
    ctx, _state = _cleanup_ctx()
    ctx.graph.aupdate_state.side_effect = RuntimeError("checkpoint unavailable")

    await _cleanup_graph_drive_files(
        "message-1",
        ctx,
        fallback_attachment_tokens=[],
        preserve_attachment_tokens=["keep-token"],
        _state_lock_held=True,
    )

    ctx.graph.aupdate_state.assert_awaited_once()


@pytest.mark.asyncio
async def test_unguarded_cleanup_readback_error_is_best_effort() -> None:
    ctx, _state = _cleanup_ctx()
    with patch(
        "src.exchange_service._snapshot_cleanup_handles",
        new=AsyncMock(side_effect=RuntimeError("readback unavailable")),
    ) as snapshot:
        await _cleanup_graph_drive_files(
            "message-1",
            ctx,
            fallback_attachment_tokens=[],
            preserve_attachment_tokens=["keep-token"],
            _state_lock_held=True,
        )

    snapshot.assert_awaited_once_with("message-1", ctx)


@pytest.mark.asyncio
async def test_unguarded_cleanup_confirmation_mismatch_is_non_destructive() -> None:
    ctx, state = _cleanup_ctx()
    with (
        patch(
            "src.exchange_service._snapshot_cleanup_handles",
            new=AsyncMock(return_value=CleanupHandleSnapshot()),
        ),
        patch("src.exchange_service.lark_app.delete_file_from_drive") as delete_file,
    ):
        await _cleanup_graph_drive_files(
            "message-1",
            ctx,
            fallback_attachment_tokens=[],
            preserve_attachment_tokens=["keep-token"],
            _state_lock_held=True,
        )

    update = ctx.graph.aupdate_state.await_args.args[1]
    assert update["attachment_tokens"] == ["keep-token"]
    assert state.values["attachment_tokens"] == ["keep-token"]
    delete_file.assert_not_called()
