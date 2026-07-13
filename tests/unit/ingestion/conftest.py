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
        source=IngressSource.WEBHOOK,
        raw_event_type="NewMailEvent",
        kind=ChangeKind.CREATE,
        external_email_id="exchange-message-1",
        folder="INBOX",
        source_version="version-1",
        dedupe_key="a" * 64,
        payload={"routing": {"folder_aliases": ["INBOX"]}},
        source_event_at=ingestion_time,
        processing_policy=ProcessingPolicy.FULL,
    )
