from unittest.mock import Mock, patch

import pytest

from src.exchange_service import _routing_evidence_hits
from src.router.context import RoutingEvidenceBundle


@pytest.mark.asyncio
async def test_routing_retrieval_bundle_is_shared_with_both_route_tiers():
    retriever = Mock()
    retriever.search_by_thread.return_value = [
        {
            "id": "thread-1",
            "sender": "owner@example.com",
            "subject": "Earlier task",
            "body": "Earlier body",
        }
    ]
    retriever.search.return_value = [
        {
            "id": "semantic-1",
            "sender": "sender@example.com",
            "subject": "Related task",
            "body": "Related body",
        }
    ]

    with patch("src.exchange_service.get_retriever", return_value=retriever):
        bundle = await _routing_evidence_hits(
            {
                "thread_id": "thread-1",
                "subject": "Current task",
                "body": "Current body",
                "sender": "sender@example.com",
            },
            email_id="current",
            _effect_boundary=None,
        )

    assert isinstance(bundle, RoutingEvidenceBundle)
    assert bundle.status == "available"
    assert [item["id"] for item in bundle.hits] == ["thread-1", "semantic-1"]
    retriever.search_by_thread.assert_called_once()
    retriever.search.assert_called_once()


@pytest.mark.asyncio
async def test_routing_retrieval_failure_is_not_presented_as_no_history():
    retriever = Mock()
    retriever.search_by_thread.side_effect = RuntimeError("qdrant unavailable")

    with patch("src.exchange_service.get_retriever", return_value=retriever):
        bundle = await _routing_evidence_hits(
            {
                "thread_id": "thread-1",
                "subject": "Current task",
                "body": "Current body",
            },
            email_id="current",
            _effect_boundary=None,
        )

    assert bundle.status == "unavailable"
    assert bundle.hits == ()


@pytest.mark.asyncio
async def test_partial_routing_retrieval_preserves_thread_evidence():
    retriever = Mock()
    retriever.search_by_thread.return_value = [
        {
            "id": "thread-1",
            "sender": "owner@example.com",
            "subject": "Earlier task",
            "body": "Earlier body",
        }
    ]
    retriever.search.side_effect = RuntimeError("embedding unavailable")

    with patch("src.exchange_service.get_retriever", return_value=retriever):
        bundle = await _routing_evidence_hits(
            {
                "thread_id": "thread-1",
                "subject": "Current task",
                "body": "Current body",
            },
            email_id="current",
            _effect_boundary=None,
        )

    assert bundle.status == "partial"
    assert [item["id"] for item in bundle.hits] == ["thread-1"]
