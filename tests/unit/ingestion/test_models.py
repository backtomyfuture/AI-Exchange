from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta, timezone, tzinfo

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


class _InvalidOffsetTimezone(tzinfo):
    def utcoffset(self, _value: datetime | None) -> timedelta:  # type: ignore[override]
        return "invalid-offset"  # type: ignore[return-value]


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
        "ignored",
    }
    assert {value.value for value in InboxStatus} == {
        "pending",
        "retry_wait",
        "leased",
        "completed",
        "dead_letter",
        "manual_review",
    }
    assert {value.value for value in InboxDispositionStatus} == {
        "retry_wait",
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


def test_normalized_event_exposes_recursive_plain_payload_for_jsonb_storage() -> None:
    event = _event(
        payload={
            "routing": {"folder_aliases": ["INBOX"]},
            "nested": [{"unicode": "合成邮件"}],
        }
    )

    storage_payload = event.payload_for_storage()

    assert type(storage_payload) is dict
    assert type(storage_payload["routing"]) is dict
    assert type(storage_payload["routing"]["folder_aliases"]) is list
    assert type(storage_payload["nested"]) is list
    assert type(storage_payload["nested"][0]) is dict
    assert (
        json.loads(json.dumps(storage_payload, ensure_ascii=False)) == storage_payload
    )


def test_normalized_event_requires_explicit_processing_policy() -> None:
    values: dict[str, object] = {
        "account_id": 8,
        "source": IngressSource.WEBHOOK,
        "raw_event_type": "NewMailEvent",
        "kind": ChangeKind.CREATE,
        "external_email_id": "exchange-message-1",
        "folder": "INBOX",
        "source_version": "version-1",
        "dedupe_key": "a" * 64,
        "payload": {},
    }

    with pytest.raises(TypeError, match="processing_policy"):
        NormalizedIngressEvent(**values)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("account_id", 0),
        ("account_id", True),
        ("account_id", 2**63),
        ("source", 1),
        ("source", "mailbox"),
        ("raw_event_type", " "),
        ("raw_event_type", "event\x00type"),
        ("raw_event_type", "x" * 129),
        ("kind", "move"),
        ("external_email_id", ""),
        ("external_email_id", "\ud800"),
        ("external_email_id", "x" * 1025),
        ("folder", "\n"),
        ("folder", "x" * 513),
        ("source_version", ""),
        ("source_version", "x" * 513),
        ("dedupe_key", "A" * 64),
        ("dedupe_key", "a" * 63),
        ("source_event_at", datetime(2026, 7, 12)),
        (
            "source_event_at",
            datetime(1, 1, 1, tzinfo=timezone(timedelta(hours=14))),
        ),
        (
            "source_event_at",
            datetime(
                9999,
                12,
                31,
                23,
                59,
                59,
                tzinfo=timezone(-timedelta(hours=14)),
            ),
        ),
        (
            "source_event_at",
            datetime(2026, 7, 12, tzinfo=_InvalidOffsetTimezone()),
        ),
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
        {"nul_value": "not-storable\x00in-jsonb"},
        {"nul_key\x00": "not-storable"},
        {"surrogate_value": "\ud800"},
        {"surrogate_key\ud800": "not-storable"},
        {"too_large": "x" * (256 * 1024)},
    ],
)
def test_normalized_event_rejects_non_json_or_oversized_payload(
    payload: object,
) -> None:
    with pytest.raises(ValueError):
        _event(payload=payload)


def test_normalized_event_rejects_payload_that_postgres_jsonb_expands_past_limit() -> (
    None
):
    payload = {"metadata": [1e-300] * 1_000}

    with pytest.raises(ValueError, match="byte limit"):
        _event(payload=payload)


def test_normalized_event_accepts_postgres_jsonb_expanded_numeric_payload_under_limit() -> (
    None
):
    event = _event(payload={"metadata": [1e-300] * 800})

    assert len(event.payload["metadata"]) == 800


def test_normalized_event_rejects_cyclic_payload() -> None:
    payload: dict[str, object] = {}
    payload["cycle"] = payload

    with pytest.raises(ValueError, match="cycle"):
        _event(payload=payload)


@pytest.mark.parametrize("target", ["event", "sync_change"])
def test_json_models_reject_excessive_nesting_as_validation_error(
    target: str,
) -> None:
    payload: dict[str, object] = {}
    nested = payload
    for _ in range(600):
        child: dict[str, object] = {}
        nested["child"] = child
        nested = child

    with pytest.raises(ValueError, match="valid UTF-8 JSON"):
        if target == "event":
            _event(payload=payload)
        else:
            SyncChange(ChangeKind.CREATE, "exchange-message-1", payload)


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
        {"external_email_id": "message\x1f"},
        {"external_email_id": "message\x7f"},
        {"external_email_id": "message\x80"},
        {"external_email_id": "\ud800"},
        {"external_email_id": "x" * 1025},
        {"item": []},
        {"item": {"bad": object()}},
        {"item": {"subject": "not-storable\x00in-jsonb"}},
        {"item": {"nul_key\x00": "not-storable"}},
        {"item": {"subject": "\ud800"}},
        {"item": {"surrogate_key\ud800": "not-storable"}},
        {"source_version": ""},
        {"source_version": "\ud800"},
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
    batch = SyncBatch(
        contract_version="exchange_sync_contract_v2",
        cursor="cursor-1",
        changes=changes,
        includes_last=False,
    )
    changes.append(SyncChange(ChangeKind.DELETE, "exchange-message-2", None))

    assert batch.contract_version == "exchange_sync_contract_v2"
    assert isinstance(batch.changes, tuple)
    assert len(batch.changes) == 1
    assert batch.includes_last is False
    with pytest.raises(FrozenInstanceError):
        batch.cursor = "cursor-2"  # type: ignore[misc]


def test_sync_batch_rejects_contract_version_string_subclass() -> None:
    class ContractVersion(str):
        pass

    with pytest.raises(ValueError, match="contract_version"):
        SyncBatch(
            contract_version=ContractVersion("exchange_sync_contract_v2"),
            cursor="cursor-1",
            changes=(),
            includes_last=False,
        )


def test_sync_batch_rejects_non_sequence_without_iterating_it() -> None:
    iterated: list[bool] = []

    def generated_changes():
        iterated.append(True)
        yield SyncChange(ChangeKind.CREATE, "exchange-message-1", {})

    with pytest.raises(ValueError, match="sequence"):
        SyncBatch(
            contract_version="exchange_sync_contract_v2",
            cursor="cursor-1",
            changes=generated_changes(),
            includes_last=False,
        )

    assert iterated == []


def test_sync_batch_bounds_iteration_when_sequence_misreports_length() -> None:
    visited: list[int] = []

    class MisreportedSequence(Sequence[SyncChange]):
        def __len__(self) -> int:
            return 1

        def __getitem__(self, index: int) -> SyncChange:
            if index > 500:
                raise AssertionError("iteration exceeded the bounded probe")
            visited.append(index)
            return SyncChange(ChangeKind.CREATE, f"exchange-message-{index}", {})

    with pytest.raises(ValueError, match="at most 500"):
        SyncBatch(
            contract_version="exchange_sync_contract_v2",
            cursor="cursor-1",
            changes=MisreportedSequence(),
            includes_last=False,
        )

    assert visited == list(range(501))


def test_sync_batch_rejects_sync_change_subclass() -> None:
    class DerivedSyncChange(SyncChange):
        pass

    with pytest.raises(ValueError, match="only SyncChange"):
        SyncBatch(
            contract_version="exchange_sync_contract_v2",
            cursor="cursor-1",
            changes=(
                DerivedSyncChange(
                    ChangeKind.CREATE,
                    "exchange-message-1",
                    {},
                ),
            ),
            includes_last=False,
        )


def test_sync_change_rejects_external_email_id_string_subclass() -> None:
    class DerivedEmailId(str):
        pass

    with pytest.raises(ValueError, match="external_email_id"):
        SyncChange(
            ChangeKind.CREATE,
            DerivedEmailId("exchange-message-1"),
            {},
        )


@pytest.mark.parametrize(
    "kwargs",
    [
        {"contract_version": "exchange_sync_contract_v1"},
        {"cursor": ""},
        {"cursor": " cursor-1 "},
        {"cursor": "cursor\x1f1"},
        {"cursor": "cursor\x7f1"},
        {"cursor": "cursor-\ud800"},
        {"cursor": "x" * 8193},
        {"changes": "not-a-sequence-of-changes"},
        {"changes": [object()]},
        {
            "changes": [
                SyncChange(ChangeKind.CREATE, f"message-{index}", {})
                for index in range(501)
            ]
        },
        {"includes_last": 1},
    ],
)
def test_sync_batch_rejects_invalid_inputs(kwargs: dict[str, object]) -> None:
    values: dict[str, object] = {
        "contract_version": "exchange_sync_contract_v2",
        "cursor": "cursor-1",
        "changes": (),
        "includes_last": False,
    }
    values.update(kwargs)
    with pytest.raises(ValueError):
        SyncBatch(**values)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "changes",
    [
        [
            SyncChange(ChangeKind.CREATE, "message-1", {"subject": "first"}),
            SyncChange(ChangeKind.CREATE, "message-1", {"subject": "first"}),
        ],
        [
            SyncChange(ChangeKind.UPDATE, "message-1", {"subject": "first"}),
            SyncChange(ChangeKind.UPDATE, "message-1", {"subject": "second"}),
        ],
    ],
)
def test_sync_batch_rejects_duplicate_kind_and_email_identity(
    changes: list[SyncChange],
) -> None:
    with pytest.raises(ValueError, match="duplicate"):
        SyncBatch(
            contract_version="exchange_sync_contract_v2",
            cursor="cursor-1",
            changes=changes,
            includes_last=True,
        )


def test_sync_batch_preserves_wire_order_for_distinct_identities() -> None:
    changes = [
        SyncChange(ChangeKind.UPDATE, "message-2", {"subject": "second"}),
        SyncChange(ChangeKind.CREATE, "message-1", {"subject": "first"}),
        SyncChange(ChangeKind.DELETE, "message-2", None),
    ]

    batch = SyncBatch(
        contract_version="exchange_sync_contract_v2",
        cursor="opaque/%2Bcursor=",
        changes=changes,
        includes_last=True,
    )

    assert batch.cursor == "opaque/%2Bcursor="
    assert batch.changes == tuple(changes)


def test_sync_batch_accepts_exact_global_maximum_of_500_changes() -> None:
    changes = tuple(
        SyncChange(ChangeKind.CREATE, f"message-{index}", {})
        for index in range(500)
    )

    batch = SyncBatch(
        contract_version="exchange_sync_contract_v2",
        cursor="cursor-500",
        changes=changes,
        includes_last=True,
    )

    assert batch.changes == changes


def test_inbox_lease_includes_received_and_expiry_times(
    normalized_event: NormalizedIngressEvent,
    ingestion_time: datetime,
) -> None:
    lease = InboxLease(
        id="00000000-0000-4000-8000-000000000001",
        account_id=8,
        pipeline_name="durable_v1",
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
    assert lease.pipeline_name == "durable_v1"
    with pytest.raises(FrozenInstanceError):
        lease.attempts = 2  # type: ignore[misc]


def test_inbox_lease_requires_pipeline_name(
    normalized_event: NormalizedIngressEvent,
    ingestion_time: datetime,
) -> None:
    with pytest.raises(TypeError, match="pipeline_name"):
        InboxLease(
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


@pytest.mark.parametrize(
    "kwargs",
    [
        {"id": "not-a-uuid"},
        {"id": 1},
        {"account_id": 0},
        {"pipeline_name": ""},
        {"pipeline_name": " durable_v1"},
        {"pipeline_name": "durable_v1 "},
        {"pipeline_name": "x" * 65},
        {"pipeline_name": 1},
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
        "pipeline_name": "durable_v1",
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
        "pipeline_name": "durable_v1",
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
        manual_review=5,
        oldest_pending_seconds=5,
    )
    assert stats.manual_review == 5
    assert stats.oldest_pending_seconds == 5.0

    for invalid in (
        {"pending": -1},
        {"retry_wait": True},
        {"manual_review": -1},
        {"oldest_pending_seconds": -0.1},
        {"oldest_pending_seconds": float("nan")},
        {"oldest_pending_seconds": 10**1000},
    ):
        values: dict[str, object] = {
            "pending": 0,
            "retry_wait": 0,
            "leased": 0,
            "dead_letter": 0,
            "manual_review": 0,
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
    manual = InboxDisposition(
        status=InboxDispositionStatus.MANUAL_REVIEW,
        attempts=2,
        available_at=None,
        safe_error_code="external_effect_outcome_unknown",
    )

    assert retry.status is InboxDispositionStatus.RETRY_WAIT
    assert dead.status is InboxDispositionStatus.DEAD_LETTER
    assert manual.status is InboxDispositionStatus.MANUAL_REVIEW

    for invalid in (
        {"status": "completed"},
        {"attempts": -1},
        {"status": "retry_wait", "available_at": None},
        {"status": "dead_letter", "available_at": ingestion_time},
        {"status": "manual_review", "available_at": ingestion_time},
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
