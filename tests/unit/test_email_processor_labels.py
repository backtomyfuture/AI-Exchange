"""Tests for EmailProcessor.update_email_labels (Tier 2 substrate)."""

from types import SimpleNamespace
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
    proc.init_collection = MagicMock()
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


def test_update_email_labels_writes_payload_and_copies_routing_sample(processor):
    processor.qdrant_client.set_payload.return_value = MagicMock()
    source = SimpleNamespace(
        id="point-1",
        vector=[0.1, 0.2],
        payload={"id": "msg-1", "subject": "hello", "sender": "a@b.com"},
    )
    processor.qdrant_client.scroll.return_value = ([source], None)

    ok = processor.update_email_labels(
        "msg-1",
        route_decision=_decision(),
        priority="P0",
        intent="审批",
        need_reply=True,
        eligible_for_tier2=True,
    )
    assert ok is True
    processor.init_collection.assert_called_once()
    set_payload = processor.qdrant_client.set_payload
    set_payload.assert_called_once()
    assert set_payload.call_args.kwargs["collection_name"] == processor.collection_name
    assert set_payload.call_args.kwargs["payload"] == {
        "route_decision": _decision(),
        "priority": "P0",
        "intent": "审批",
        "need_reply": True,
        "eligible_for_tier2": True,
    }
    assert set_payload.call_args.kwargs["wait"] is False

    upsert = processor.qdrant_client.upsert
    upsert.assert_called_once()
    assert upsert.call_args.kwargs["collection_name"] == processor.routing_collection_name
    copied = upsert.call_args.kwargs["points"]
    assert len(copied) == 1
    assert copied[0].id == "point-1"
    assert copied[0].vector == [0.1, 0.2]
    assert copied[0].payload["id"] == "msg-1"
    assert copied[0].payload["subject"] == "hello"
    assert copied[0].payload["route_decision"] == _decision()
    assert copied[0].payload["eligible_for_tier2"] is True


def test_update_email_labels_still_succeeds_when_source_vectors_are_missing(processor):
    processor.qdrant_client.scroll.return_value = ([], None)

    ok = processor.update_email_labels("msg-1", route_decision=_decision())
    assert ok is True
    processor.qdrant_client.set_payload.assert_called_once()
    processor.qdrant_client.upsert.assert_not_called()


def test_update_email_labels_noop_for_empty_id(processor):
    assert processor.update_email_labels("", route_decision=_decision()) is False
    processor.qdrant_client.set_payload.assert_not_called()
    processor.qdrant_client.upsert.assert_not_called()


def test_update_email_labels_noop_when_no_fields(processor):
    assert processor.update_email_labels("msg-1") is False
    processor.qdrant_client.set_payload.assert_not_called()
    processor.qdrant_client.upsert.assert_not_called()


def test_update_email_labels_swallows_qdrant_errors(processor):
    processor.qdrant_client.set_payload.side_effect = ConnectionError("down")
    assert processor.update_email_labels("msg-1", route_decision=_decision()) is False
