from __future__ import annotations

from collections.abc import Mapping
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta, timezone

import pytest

from src.domain.email_state import PipelineGenerationState
from src.ingestion.models import (
    ChangeKind,
    InboxDisposition,
    InboxDispositionStatus,
    InboxLease,
    InboxStats,
    InboxStatus,
    IngressReceipt,
    IngressSource,
    NormalizedIngressEvent,
    PipelineGeneration,
    ProcessingPolicy,
    SyncBatch,
    SyncChange,
    SyncCursorStatus,
)


def _event(**overrides: object) -> NormalizedIngressEvent:
    values: dict[str, object] = {
        "account_id": 8,
        "source": IngressSource.WEBHOOK,
        "raw_event_type": "NewMailEvent",
        "kind": ChangeKind.CREATE,
        "external_email_id": "exchange-message-1",
        "folder": "INBOX",
        "source_version": "version-1",
        "dedupe_key": "a" * 64,
        "payload": {"routing": {"folder_aliases": ["INBOX"]}},
        "source_event_at": datetime(2026, 7, 12, 3, 4, 5, tzinfo=UTC),
        "processing_policy": ProcessingPolicy.FULL,
    }
    values.update(overrides)
    return NormalizedIngressEvent(**values)  # type: ignore[arg-type]


def test_ingestion_enums_lock_database_vocabulary() -> None:
    assert {value.value for value in ChangeKind} == {
        "create",
        "update",
        "read",
        "delete",
    }
    assert {value.value for value in IngressSource} == {
        "webhook",
        "sync",
        "backfill",
    }
    assert {value.value for value in ProcessingPolicy} == {
        "full",
        "archive",
        "metadata_only",
        "historical_suppressed",
    }
    assert {value.value for value in InboxStatus} == {
        "pending",
        "retry_wait",
        "leased",
        "completed",
        "dead_letter",
        "manual_review",
    }
    assert {value.value for value in SyncCursorStatus} == {
        "active",
        "reset_required",
        "cold_start_pending",
        "blocked_contract",
    }


def test_normalized_event_normalizes_enums_time_and_deep_freezes_payload() -> None:
    mutable_payload = {
        "routing": {
            "folder_aliases": ["INBOX"],
            "metadata": {"priority": 1},
        },
        "ratio": 0.5,
    }
    event = _event(
        source="webhook",
        kind="create",
        processing_policy="full",
        source_event_at=datetime(
            2026,
            7,
            12,
            11,
            4,
            5,
            tzinfo=timezone(timedelta(hours=8)),
        ),
        payload=mutable_payload,
    )

    mutable_payload["routing"]["folder_aliases"].append("JUNK")
    mutable_payload["routing"]["metadata"]["priority"] = 99

    assert event.source is IngressSource.WEBHOOK
    assert event.kind is ChangeKind.CREATE
    assert event.processing_policy is ProcessingPolicy.FULL
    assert event.source_event_at == datetime(2026, 7, 12, 3, 4, 5, tzinfo=UTC)
    assert event.payload["routing"]["folder_aliases"] == ("INBOX",)
    assert event.payload["routing"]["metadata"]["priority"] == 1
    assert event.payload["ratio"] == 0.5
    assert isinstance(event.payload, Mapping)
    with pytest.raises(TypeError):
        event.payload["new"] = "value"  # type: ignore[index]
    with pytest.raises(TypeError):
        event.payload["routing"]["metadata"]["priority"] = 2  # type: ignore[index]
    with pytest.raises(FrozenInstanceError):
        event.account_id = 9  # type: ignore[misc]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("account_id", 0),
        ("account_id", True),
        ("source", 1),
        ("source", "mailbox"),
        ("raw_event_type", " "),
        ("raw_event_type", "event\x00type"),
        ("raw_event_type", "x" * 129),
        ("kind", "move"),
        ("external_email_id", ""),
        ("external_email_id", "x" * 1025),
        ("folder", "\n"),
        ("folder", "x" * 513),
        ("source_version", ""),
        ("source_version", "x" * 513),
        ("dedupe_key", "A" * 64),
        ("dedupe_key", "a" * 63),
        ("source_event_at", datetime(2026, 7, 12)),
        ("payload", []),
        ("processing_policy", "execute_everything"),
    ],
)
def test_normalized_event_rejects_invalid_inputs(field: str, value: object) -> None:
    with pytest.raises(ValueError):
        _event(**{field: value})


@pytest.mark.parametrize(
    "payload",
    [
        {1: "non-string key"},
        {"nan": float("nan")},
        {"infinity": float("inf")},
        {"unsupported": object()},
        {"too_large": "x" * (256 * 1024)},
    ],
)
def test_normalized_event_rejects_non_json_or_oversized_payload(
    payload: object,
) -> None:
    with pytest.raises(ValueError):
        _event(payload=payload)


def test_normalized_event_rejects_cyclic_payload() -> None:
    payload: dict[str, object] = {}
    payload["cycle"] = payload

    with pytest.raises(ValueError, match="cycle"):
        _event(payload=payload)


def test_pipeline_generation_is_frozen_and_validated() -> None:
    generation = PipelineGeneration(
        account_id=8,
        generation=3,
        pipeline_name="durable-v2",
        state="current_ingress",
        fencing_token=5,
    )

    assert generation.state is PipelineGenerationState.CURRENT_INGRESS
    with pytest.raises(FrozenInstanceError):
        generation.fencing_token = 6  # type: ignore[misc]

    for invalid in (
        {"account_id": 0},
        {"generation": False},
        {"pipeline_name": ""},
        {"pipeline_name": "x" * 65},
        {"state": "paused"},
        {"fencing_token": -1},
    ):
        values: dict[str, object] = {
            "account_id": 8,
            "generation": 3,
            "pipeline_name": "durable-v2",
            "state": PipelineGenerationState.CURRENT_INGRESS,
            "fencing_token": 5,
        }
        values.update(invalid)
        with pytest.raises(ValueError):
            PipelineGeneration(**values)  # type: ignore[arg-type]


def test_sync_change_deep_freezes_item_and_normalizes_kind() -> None:
    mutable_item = {"headers": [{"name": "subject", "value": "safe-ref"}]}
    change = SyncChange(
        kind="update",
        external_email_id="exchange-message-1",
        item=mutable_item,
        source_version="version-2",
    )
    mutable_item["headers"][0]["value"] = "changed"

    assert change.kind is ChangeKind.UPDATE
    assert change.item is not None
    assert change.item["headers"][0]["value"] == "safe-ref"
    with pytest.raises(TypeError):
        change.item["headers"][0]["value"] = "again"  # type: ignore[index]


@pytest.mark.parametrize(
    "kwargs",
    [
        {"kind": "move"},
        {"external_email_id": ""},
        {"external_email_id": "x" * 1025},
        {"item": []},
        {"item": {"bad": object()}},
        {"source_version": ""},
    ],
)
def test_sync_change_rejects_invalid_inputs(kwargs: dict[str, object]) -> None:
    values: dict[str, object] = {
        "kind": ChangeKind.CREATE,
        "external_email_id": "exchange-message-1",
        "item": {},
        "source_version": None,
    }
    values.update(kwargs)
    with pytest.raises(ValueError):
        SyncChange(**values)  # type: ignore[arg-type]


def test_sync_batch_copies_changes_to_an_immutable_tuple() -> None:
    changes = [SyncChange(ChangeKind.CREATE, "exchange-message-1", {})]
    batch = SyncBatch(cursor="cursor-1", changes=changes, is_full=False)
    changes.append(SyncChange(ChangeKind.DELETE, "exchange-message-2", None))

    assert isinstance(batch.changes, tuple)
    assert len(batch.changes) == 1
    with pytest.raises(FrozenInstanceError):
        batch.cursor = "cursor-2"  # type: ignore[misc]


@pytest.mark.parametrize(
    "kwargs",
    [
        {"cursor": ""},
        {"cursor": "x" * 8193},
        {"changes": "not-a-sequence-of-changes"},
        {"changes": [object()]},
        {"is_full": 1},
    ],
)
def test_sync_batch_rejects_invalid_inputs(kwargs: dict[str, object]) -> None:
    values: dict[str, object] = {
        "cursor": "cursor-1",
        "changes": (),
        "is_full": False,
    }
    values.update(kwargs)
    with pytest.raises(ValueError):
        SyncBatch(**values)  # type: ignore[arg-type]


def test_inbox_lease_includes_received_and_expiry_times(
    normalized_event: NormalizedIngressEvent,
    ingestion_time: datetime,
) -> None:
    lease = InboxLease(
        id="00000000-0000-4000-8000-000000000001",
        account_id=8,
        generation=3,
        fencing_token=5,
        lease_owner="worker-1",
        attempts=1,
        event=normalized_event,
        received_at=ingestion_time,
        lease_until=ingestion_time + timedelta(seconds=30),
    )

    assert lease.received_at == ingestion_time
    assert lease.lease_until == ingestion_time + timedelta(seconds=30)
    with pytest.raises(FrozenInstanceError):
        lease.attempts = 2  # type: ignore[misc]


@pytest.mark.parametrize(
    "kwargs",
    [
        {"id": "not-a-uuid"},
        {"id": 1},
        {"account_id": 0},
        {"generation": 0},
        {"fencing_token": True},
        {"lease_owner": ""},
        {"lease_owner": "x" * 129},
        {"attempts": -1},
        {"event": object()},
        {"received_at": datetime(2026, 7, 12)},
        {"received_at": "not-a-datetime"},
        {"lease_until": datetime(2026, 7, 12)},
    ],
)
def test_inbox_lease_rejects_invalid_fields(
    normalized_event: NormalizedIngressEvent,
    ingestion_time: datetime,
    kwargs: dict[str, object],
) -> None:
    values: dict[str, object] = {
        "id": "00000000-0000-4000-8000-000000000001",
        "account_id": 8,
        "generation": 3,
        "fencing_token": 5,
        "lease_owner": "worker-1",
        "attempts": 1,
        "event": normalized_event,
        "received_at": ingestion_time,
        "lease_until": ingestion_time + timedelta(seconds=30),
    }
    values.update(kwargs)
    with pytest.raises(ValueError):
        InboxLease(**values)  # type: ignore[arg-type]


def test_inbox_lease_rejects_account_mismatch_and_non_future_expiry(
    normalized_event: NormalizedIngressEvent,
    ingestion_time: datetime,
) -> None:
    common: dict[str, object] = {
        "id": "00000000-0000-4000-8000-000000000001",
        "generation": 3,
        "fencing_token": 5,
        "lease_owner": "worker-1",
        "attempts": 1,
        "event": normalized_event,
        "received_at": ingestion_time,
        "lease_until": ingestion_time + timedelta(seconds=30),
    }
    with pytest.raises(ValueError, match="account"):
        InboxLease(account_id=9, **common)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="lease_until"):
        InboxLease(
            account_id=8,
            **{**common, "lease_until": ingestion_time},
        )  # type: ignore[arg-type]


def test_inbox_stats_validate_nonnegative_finite_counts() -> None:
    stats = InboxStats(
        pending=1,
        retry_wait=2,
        leased=3,
        dead_letter=4,
        oldest_pending_seconds=5,
    )
    assert stats.oldest_pending_seconds == 5.0

    for invalid in (
        {"pending": -1},
        {"retry_wait": True},
        {"oldest_pending_seconds": -0.1},
        {"oldest_pending_seconds": float("nan")},
        {"oldest_pending_seconds": 10**1000},
    ):
        values: dict[str, object] = {
            "pending": 0,
            "retry_wait": 0,
            "leased": 0,
            "dead_letter": 0,
            "oldest_pending_seconds": 0,
        }
        values.update(invalid)
        with pytest.raises(ValueError):
            InboxStats(**values)  # type: ignore[arg-type]


def test_inbox_disposition_locks_retry_and_dead_letter_state_matrix(
    ingestion_time: datetime,
) -> None:
    retry = InboxDisposition(
        status="retry_wait",
        attempts=2,
        available_at=ingestion_time,
        safe_error_code="exchange_temporarily_unavailable",
    )
    dead = InboxDisposition(
        status=InboxDispositionStatus.DEAD_LETTER,
        attempts=5,
        available_at=None,
        safe_error_code="attempt_budget_exhausted",
    )

    assert retry.status is InboxDispositionStatus.RETRY_WAIT
    assert dead.status is InboxDispositionStatus.DEAD_LETTER

    for invalid in (
        {"status": "completed"},
        {"attempts": -1},
        {"status": "retry_wait", "available_at": None},
        {"status": "dead_letter", "available_at": ingestion_time},
        {"available_at": datetime(2026, 7, 12)},
        {"safe_error_code": ""},
        {"safe_error_code": "x" * 65},
    ):
        values: dict[str, object] = {
            "status": InboxDispositionStatus.RETRY_WAIT,
            "attempts": 2,
            "available_at": ingestion_time,
            "safe_error_code": "temporary_failure",
        }
        values.update(invalid)
        with pytest.raises(ValueError):
            InboxDisposition(**values)  # type: ignore[arg-type]


def test_ingress_receipt_is_frozen_and_validates_uuid_and_boolean() -> None:
    receipt = IngressReceipt(
        inbox_id="00000000-0000-4000-8000-000000000001",
        duplicate=False,
    )
    assert receipt.inbox_id == "00000000-0000-4000-8000-000000000001"
    with pytest.raises(FrozenInstanceError):
        receipt.duplicate = True  # type: ignore[misc]

    with pytest.raises(ValueError):
        IngressReceipt(inbox_id="bad-id", duplicate=False)
    with pytest.raises(ValueError):
        IngressReceipt(
            inbox_id="00000000-0000-4000-8000-000000000001",
            duplicate=0,  # type: ignore[arg-type]
        )
