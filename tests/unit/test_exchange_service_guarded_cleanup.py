from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from src.email_feishu_delivery import (
    EmailDeliveryDisposition,
    EmailFeishuDelivery,
    LarkCardDelivery,
    ReadNotificationRequest,
)
from src.ingestion.processing import ExternalEffectBoundary, ProcessingEffectScope


async def _allow_effect(_kind: str, _ordinal: int, _target_hash: str) -> None:
    return None


def _boundary() -> ExternalEffectBoundary:
    return ExternalEffectBoundary(
        scope=ProcessingEffectScope(
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


def _stateful_graph(values: dict[str, object]):
    async def get_state(_config):
        return SimpleNamespace(values=dict(values), next=())

    async def update_state(_config, delta, **_kwargs):
        values.update(delta)

    return SimpleNamespace(
        aget_state=AsyncMock(side_effect=get_state),
        aupdate_state=AsyncMock(side_effect=update_state),
    )


@pytest.mark.asyncio
async def test_confirmed_card_retains_replaced_pdf_when_guarded_cleanup_is_not_confirmed():
    values: dict[str, object] = {"attachment_tokens": [], "pdf_token": "old-pdf"}
    graph = _stateful_graph(values)
    database = AsyncMock()

    async def generate_pdf(*_args, **_kwargs):
        return {"url": "https://feishu.example/new", "file_token": "new-pdf"}

    delivery = EmailFeishuDelivery(
        database=database,
        graph=graph,
        graph_dependencies=MagicMock(),
        generate_pdf=generate_pdf,
        send_card=lambda *_args: LarkCardDelivery(True, True),
        upload_file=MagicMock(),
        delete_file=MagicMock(return_value=False),
    )

    outcome = await delivery.deliver(
        ReadNotificationRequest(
            email_id="mail-1",
            email_data={"id": "mail-1", "attachments": []},
            classification={"need_reply": False},
            context=(),
            routing_log=(),
        ),
        _boundary(),
    )

    assert outcome.disposition is EmailDeliveryDisposition.CONFIRMED
    assert values["pdf_token"] == "new-pdf"
    assert values["attachment_tokens"] == ["old-pdf"]


@pytest.mark.asyncio
async def test_known_rejected_card_moves_its_pdf_to_retry_safe_cleanup_before_delete():
    values: dict[str, object] = {"attachment_tokens": [], "pdf_token": None}
    graph = _stateful_graph(values)
    database = AsyncMock()

    async def generate_pdf(*_args, **_kwargs):
        return {"url": "https://feishu.example/new", "file_token": "new-pdf"}

    delivery = EmailFeishuDelivery(
        database=database,
        graph=graph,
        graph_dependencies=MagicMock(),
        generate_pdf=generate_pdf,
        send_card=lambda *_args: LarkCardDelivery(False, True),
        upload_file=MagicMock(),
        delete_file=MagicMock(return_value=False),
    )

    outcome = await delivery.deliver(
        ReadNotificationRequest(
            email_id="mail-1",
            email_data={"id": "mail-1", "attachments": []},
            classification={"need_reply": False},
            context=(),
            routing_log=(),
        ),
        effect_boundary=None,
    )

    assert outcome.disposition is EmailDeliveryDisposition.KNOWN_FAILURE
    assert values["pdf_token"] is None
    assert values["attachment_tokens"] == ["new-pdf"]
