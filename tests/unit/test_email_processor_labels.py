"""Tests for EmailProcessor.update_email_labels (Tier 2 substrate)."""

from unittest.mock import MagicMock, patch

import pytest

from src.utils.email_processor import EmailProcessor
from src.router.decision import DecisionOutcome, RouteDecision, RouteProvenance, RouteTier
from src.router.tier1.schema import CanonicalRoute


@pytest.fixture
def processor():
    with patch("src.utils.email_processor.QdrantClient") as qdrant_cls:
        qdrant_cls.return_value = MagicMock()
        proc = EmailProcessor()
    yield proc


def _decision():
    return RouteDecision(
        outcome=DecisionOutcome.MATCHED,
        route=CanonicalRoute.REPLY,
        params={"reply_mode": "sender_only"},
        provenance=RouteProvenance(
            tier=RouteTier.TIER3,
            source_version="router-model-v1",
            confidence=0.9,
        ),
        reason_code="test",
    ).model_dump(mode="json")


def test_update_email_labels_writes_payload(processor):
    processor.qdrant_client.set_payload.return_value = MagicMock()

    ok = processor.update_email_labels(
        "msg-1",
        route_decision=_decision(),
        priority="P0",
        intent="审批",
        need_reply=True,
    )
    assert ok is True
    args, kwargs = processor.qdrant_client.set_payload.call_args
    assert kwargs["collection_name"] == processor.collection_name
    assert kwargs["payload"] == {
        "route_decision": _decision(),
        "priority": "P0",
        "intent": "审批",
        "need_reply": True,
    }
    assert kwargs["wait"] is False


def test_update_email_labels_noop_for_empty_id(processor):
    assert processor.update_email_labels("", route_decision=_decision()) is False
    processor.qdrant_client.set_payload.assert_not_called()


def test_update_email_labels_noop_when_no_fields(processor):
    assert processor.update_email_labels("msg-1") is False
    processor.qdrant_client.set_payload.assert_not_called()


def test_update_email_labels_swallows_qdrant_errors(processor):
    processor.qdrant_client.set_payload.side_effect = ConnectionError("down")
    assert processor.update_email_labels("msg-1", route_decision=_decision()) is False
