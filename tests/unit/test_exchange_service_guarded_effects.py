from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from src.email_feishu_delivery import (
    EmailFeishuDelivery,
    LarkCardDelivery,
    ReadNotificationRequest,
)
from src.exchange_service import (
    _delete_unclaimed_content_candidate,
    _effect_boundary_kwargs,
    _ingest_to_qdrant,
    _mark_email_read,
)
from src.ingestion.processing import (
    ExternalEffectBoundary,
    GuardedExternalEffectFailed,
    ProcessingEffectScope,
)
from src.storage import ContentRef


def _scope(email_id: str = "message-1") -> ProcessingEffectScope:
    return ProcessingEffectScope(
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


def _boundary(callback: AsyncMock | None = None):
    port = callback or AsyncMock(return_value=None)
    return ExternalEffectBoundary(_scope(), port), port


def _stateful_graph(values: dict[str, object]):
    async def get_state(_config):
        return SimpleNamespace(values=dict(values), next=())

    async def update_state(_config, delta, **_kwargs):
        values.update(delta)

    return SimpleNamespace(
        aget_state=AsyncMock(side_effect=get_state),
        aupdate_state=AsyncMock(side_effect=update_state),
    )


def test_boundary_keyword_projection_never_invents_an_unguarded_boundary() -> None:
    boundary, _port = _boundary()

    assert _effect_boundary_kwargs(None) == {}
    assert _effect_boundary_kwargs(boundary) == {"_effect_boundary": boundary}


@pytest.mark.asyncio
async def test_delivery_reuses_the_existing_boundary_for_attachment_pdf_and_card():
    boundary, port = _boundary()
    values: dict[str, object] = {"attachment_tokens": [], "pdf_token": None}
    graph = _stateful_graph(values)

    async def generate_pdf(*_args, **_kwargs):
        return {"url": "https://feishu.example/review", "file_token": "review-pdf"}

    delivery = EmailFeishuDelivery(
        database=AsyncMock(),
        graph=graph,
        graph_dependencies=MagicMock(),
        generate_pdf=generate_pdf,
        send_card=lambda *_args: LarkCardDelivery(True, True),
        upload_file=MagicMock(
            return_value={"file_token": "attachment", "url": "https://feishu.example/a"}
        ),
        delete_file=MagicMock(return_value=True),
    )

    await delivery.deliver(
        ReadNotificationRequest(
            email_id="message-1",
            email_data={
                "id": "message-1",
                "attachments": [{"name": "note.txt", "content": "YQ=="}],
            },
            classification={"need_reply": False},
            context=(),
            routing_log=(),
        ),
        boundary,
    )

    assert [call.args[:2] for call in port.await_args_list] == [
        ("feishu", 0),
        ("feishu", 32),
        ("feishu", 33),
    ]


@pytest.mark.parametrize("remote_result", [False, None])
@pytest.mark.asyncio
async def test_guarded_qdrant_requires_positive_remote_confirmation(remote_result):
    boundary, port = _boundary()
    ctx = SimpleNamespace(
        email_processor=SimpleNamespace(process_email=MagicMock(return_value=remote_result)),
        db_manager=SimpleNamespace(update_status=AsyncMock()),
    )

    with pytest.raises(GuardedExternalEffectFailed):
        await _ingest_to_qdrant(
            "message-1",
            {"id": "message-1"},
            ctx,
            _effect_boundary=boundary,
        )

    assert port.await_args.args[:2] == ("qdrant", 0)
    ctx.db_manager.update_status.assert_not_awaited()


@pytest.mark.parametrize("remote_result", [False, None])
@pytest.mark.asyncio
async def test_guarded_mark_read_requires_exact_true_confirmation(remote_result):
    boundary, port = _boundary()
    ctx = SimpleNamespace(
        exchange_client=SimpleNamespace(mark_as_read=AsyncMock(return_value=remote_result))
    )

    with pytest.raises(GuardedExternalEffectFailed):
        await _mark_email_read("message-1", ctx, _effect_boundary=boundary)

    assert port.await_args.args[:2] == ("exchange_mutation", 0)


@pytest.mark.asyncio
async def test_guarded_unclaimed_content_cleanup_preserves_cancellation_identity():
    boundary, port = _boundary()
    cancellation = asyncio.CancelledError()
    ctx = SimpleNamespace(content_store=SimpleNamespace(delete=AsyncMock(side_effect=cancellation)))
    ref = ContentRef(
        account_id=8,
        object_id="00000000-0000-4000-8000-000000000088",
        key_version="v1",
        sha256="8" * 64,
    )

    with pytest.raises(asyncio.CancelledError) as caught:
        await _delete_unclaimed_content_candidate(
            ref,
            ctx,
            reason="test",
            _effect_boundary=boundary,
        )

    assert caught.value is cancellation
    assert port.await_args.args[:2] == ("content", 2)
