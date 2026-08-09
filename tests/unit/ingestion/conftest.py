from __future__ import annotations

from datetime import UTC, datetime

import pytest

from src.ingestion.models import (
    ChangeKind,
    IngressSource,
    NormalizedIngressEvent,
    ProcessingPolicy,
)


@pytest.fixture
def ingestion_time() -> datetime:
    return datetime(2026, 7, 12, 3, 4, 5, tzinfo=UTC)


@pytest.fixture
def normalized_event(ingestion_time: datetime) -> NormalizedIngressEvent:
    return NormalizedIngressEvent(
        account_id=8,
        source=IngressSource.SYNC,
        raw_event_type="create",
        kind=ChangeKind.CREATE,
        external_email_id="exchange-message-1",
        folder="INBOX",
        source_version="version-1",
        dedupe_key="a" * 64,
        payload={"cursor": "cursor-1", "change_type": "create", "id": "exchange-message-1", "item": {}},
        source_event_at=None,
        processing_policy=ProcessingPolicy.FULL,
    )
