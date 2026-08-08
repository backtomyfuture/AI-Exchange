from __future__ import annotations

import json

import pytest
from psycopg.types.json import Jsonb

from src.ingestion.models import (
    MAX_INBOX_PAYLOAD_BYTES,
    ChangeKind,
    IngressSource,
    NormalizedIngressEvent,
    ProcessingPolicy,
)


def _event(payload: object) -> NormalizedIngressEvent:
    return NormalizedIngressEvent(
        account_id=8,
        source=IngressSource.SYNC,
        raw_event_type="create",
        kind=ChangeKind.CREATE,
        external_email_id="numeric-payload-message",
        folder="INBOX",
        source_version="version-1",
        dedupe_key="a" * 64,
        payload=payload,  # type: ignore[arg-type]
        processing_policy=ProcessingPolicy.FULL,
    )


def _postgres_jsonb_text_size(db, payload: object) -> int:
    encoded = json.dumps(payload, ensure_ascii=False, separators=(", ", ": "))
    size = db.scalar(
        "SELECT pg_catalog.octet_length((%s)::pg_catalog.jsonb::pg_catalog.text)",
        (encoded,),
    )
    assert isinstance(size, int)
    return size


@pytest.mark.integration
def test_dto_rejects_compact_numeric_payload_that_jsonb_expands_past_limit(db):
    payload = {"metadata": [1e-300] * 1_000}

    assert len(json.dumps(payload).encode("utf-8")) < MAX_INBOX_PAYLOAD_BYTES
    assert _postgres_jsonb_text_size(db, payload) > MAX_INBOX_PAYLOAD_BYTES

    with pytest.raises(ValueError, match="byte limit"):
        _event(payload)


@pytest.mark.integration
def test_dto_accepts_numeric_payload_when_expanded_jsonb_remains_under_limit(db):
    payload = {"metadata": [1e-300] * 800}

    assert _postgres_jsonb_text_size(db, payload) <= MAX_INBOX_PAYLOAD_BYTES
    event = _event(payload)

    assert len(event.payload["metadata"]) == 800


@pytest.mark.integration
def test_recursive_storage_payload_round_trips_through_psycopg_jsonb(db):
    payload = {
        "routing": {"folder_aliases": ["INBOX"]},
        "nested": [{"unicode": "合成邮件", "ratio": 0.5}],
    }
    event = _event(payload)

    stored = db.scalar(
        "SELECT (%s)::pg_catalog.jsonb",
        (Jsonb(event.payload_for_storage()),),
    )

    assert stored == payload
