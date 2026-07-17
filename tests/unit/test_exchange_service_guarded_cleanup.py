from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import pytest

from src.exchange_service import (
    CleanupHandleSnapshot,
    _cleanup_graph_drive_files,
    _delete_replaced_pdf,
)
from src.ingestion.processing import (
    ExternalEffectBoundary,
    GuardedExternalEffectFailed,
    LegacyEffectScope,
)


async def _allow_effect(_kind: str, _ordinal: int, _target_hash: str) -> None:
    return None


def _boundary() -> ExternalEffectBoundary:
    return ExternalEffectBoundary(
        scope=LegacyEffectScope(
            account_id=8,
            inbox_id=str(uuid4()),
            generation=1,
            fencing_token=1,
            attempts=0,
            email_id=str(uuid4()),
            expected_email_version=1,
            event_dedupe_key="a" * 64,
            external_email_id="mail-1",
        ),
        callback=_allow_effect,
    )


@pytest.mark.asyncio
async def test_guarded_cleanup_state_lookup_failure_prevents_remote_delete() -> None:
    ctx = SimpleNamespace(
        graph=SimpleNamespace(
            aget_state=AsyncMock(side_effect=RuntimeError("state unavailable")),
            aupdate_state=AsyncMock(),
        )
    )

    with patch(
        "src.exchange_service.lark_app.delete_file_from_drive",
        return_value=True,
    ) as delete:
        with pytest.raises(GuardedExternalEffectFailed):
            await _cleanup_graph_drive_files(
                "mail-1",
                ctx,
                fallback_attachment_tokens=["token-1"],
                _effect_boundary=_boundary(),
            )

    delete.assert_not_called()
    ctx.graph.aupdate_state.assert_not_awaited()


@pytest.mark.asyncio
async def test_guarded_cleanup_state_update_failure_is_fixed_failure() -> None:
    values = {"attachment_tokens": ["token-1"], "pdf_token": None}
    state = SimpleNamespace(values=values, next=())
    ctx = SimpleNamespace(
        graph=SimpleNamespace(
            aget_state=AsyncMock(return_value=state),
            aupdate_state=AsyncMock(side_effect=RuntimeError("write unavailable")),
        )
    )

    with patch(
        "src.exchange_service.lark_app.delete_file_from_drive",
        return_value=True,
    ) as delete:
        with pytest.raises(GuardedExternalEffectFailed):
            await _cleanup_graph_drive_files(
                "mail-1",
                ctx,
                fallback_attachment_tokens=[],
                _effect_boundary=_boundary(),
            )

    delete.assert_called_once_with("token-1")


@pytest.mark.asyncio
async def test_guarded_cleanup_state_readback_failure_is_fixed_failure() -> None:
    values = {"attachment_tokens": ["token-1"], "pdf_token": None}
    state = SimpleNamespace(values=values, next=())

    async def update(_config, delta, **_kwargs) -> None:
        values.update(delta)

    ctx = SimpleNamespace(
        graph=SimpleNamespace(
            aget_state=AsyncMock(return_value=state),
            aupdate_state=AsyncMock(side_effect=update),
        )
    )

    with (
        patch(
            "src.exchange_service.lark_app.delete_file_from_drive",
            return_value=True,
        ),
        patch(
            "src.exchange_service._snapshot_cleanup_handles",
            new=AsyncMock(side_effect=RuntimeError("readback unavailable")),
        ),
    ):
        with pytest.raises(GuardedExternalEffectFailed):
            await _cleanup_graph_drive_files(
                "mail-1",
                ctx,
                fallback_attachment_tokens=[],
                _effect_boundary=_boundary(),
            )


@pytest.mark.asyncio
async def test_guarded_cleanup_unconfirmed_state_is_fixed_failure() -> None:
    values = {"attachment_tokens": ["preserved"], "pdf_token": None}
    state = SimpleNamespace(values=values, next=())
    ctx = SimpleNamespace(
        graph=SimpleNamespace(
            aget_state=AsyncMock(return_value=state),
            aupdate_state=AsyncMock(),
        )
    )

    with patch(
        "src.exchange_service._snapshot_cleanup_handles",
        new=AsyncMock(return_value=CleanupHandleSnapshot()),
    ):
        with pytest.raises(GuardedExternalEffectFailed):
            await _cleanup_graph_drive_files(
                "mail-1",
                ctx,
                fallback_attachment_tokens=[],
                preserve_attachment_tokens=["preserved"],
                _effect_boundary=_boundary(),
            )


@pytest.mark.asyncio
async def test_guarded_replaced_pdf_state_failure_stops_after_remote_delete() -> None:
    values = {"attachment_tokens": ["old-pdf"]}
    state = SimpleNamespace(values=values)
    ctx = SimpleNamespace(
        graph=SimpleNamespace(
            aget_state=AsyncMock(return_value=state),
            aupdate_state=AsyncMock(side_effect=RuntimeError("write unavailable")),
        )
    )

    with patch(
        "src.exchange_service.lark_app.delete_file_from_drive",
        return_value=True,
    ) as delete:
        with pytest.raises(GuardedExternalEffectFailed):
            await _delete_replaced_pdf(
                "mail-1",
                ctx,
                "old-pdf",
                "new-pdf",
                _effect_boundary=_boundary(),
            )

    delete.assert_called_once_with("old-pdf")


@pytest.mark.asyncio
async def test_live_cleanup_keeps_best_effort_behavior_on_state_lookup_failure() -> (
    None
):
    ctx = SimpleNamespace(
        graph=SimpleNamespace(
            aget_state=AsyncMock(side_effect=RuntimeError("state unavailable")),
            aupdate_state=AsyncMock(),
        )
    )

    with patch(
        "src.exchange_service.lark_app.delete_file_from_drive",
        return_value=True,
    ) as delete:
        await _cleanup_graph_drive_files(
            "mail-1",
            ctx,
            fallback_attachment_tokens=["token-1"],
        )

    delete.assert_called_once_with("token-1")
