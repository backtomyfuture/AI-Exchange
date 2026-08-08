from __future__ import annotations

from types import SimpleNamespace

import pytest

from src.domain.errors import IngressValidationError
from src.ingestion.legacy_adapter import LegacyProcessingAdapter, LegacyProcessingFailed
from src.ingestion.models import (
    ChangeKind,
    IngressSource,
    ProcessingPolicy,
    SyncBatch,
    SyncChange,
)
from src.ingestion.normalization import normalize_sync_change, validate_sync_change_contract
from src.ingestion.policy import FolderScope, PolicySnapshot, ProcessingPolicyResolver
from src.ingestion.runtime import build_ingestion_runtime


def _matrix(create_policy: ProcessingPolicy = ProcessingPolicy.FULL) -> dict:
    return {
        (IngressSource.SYNC, "create", ChangeKind.CREATE): create_policy,
        (IngressSource.SYNC, "update", ChangeKind.UPDATE): ProcessingPolicy.METADATA_ONLY,
        (IngressSource.SYNC, "delete", ChangeKind.DELETE): ProcessingPolicy.METADATA_ONLY,
    }


def test_sync_is_the_only_supported_ingress_source() -> None:
    assert tuple(IngressSource) == (IngressSource.SYNC,)


def test_polling_policy_requires_the_three_sync_events() -> None:
    scope = FolderScope.configured(
        canonical_key="INBOX",
        sync_folder="INBOX",
        event_policy_matrix=_matrix(),
    )
    snapshot = PolicySnapshot(scopes=(scope,))

    assert snapshot.ready is True
    resolver = ProcessingPolicyResolver()
    assert (
        resolver.resolve(IngressSource.SYNC, "create", ChangeKind.CREATE, "INBOX", snapshot)
        is ProcessingPolicy.FULL
    )
    assert (
        resolver.resolve(IngressSource.SYNC, "delete", ChangeKind.DELETE, "INBOX", snapshot)
        is ProcessingPolicy.METADATA_ONLY
    )


def test_polling_policy_rejects_non_metadata_update_or_delete() -> None:
    invalid = _matrix()
    invalid[(IngressSource.SYNC, "update", ChangeKind.UPDATE)] = ProcessingPolicy.FULL

    with pytest.raises(ValueError, match="METADATA_ONLY"):
        FolderScope.configured(
            canonical_key="INBOX",
            sync_folder="INBOX",
            event_policy_matrix=invalid,
        )


def test_sync_change_normalizes_to_a_stable_polling_event() -> None:
    change = validate_sync_change_contract(
        {
            "change_type": "create",
            "id": "message-1",
            "item": {
                "id": "message-1",
                "subject": "status",
                "sender": "sender@example.test",
                "received_time": "2026-08-08T10:00:00",
                "is_read": False,
                "has_attachments": False,
            },
        }
    )
    event = normalize_sync_change(
        1,
        "INBOX",
        "cursor-1",
        change,
        processing_policy=ProcessingPolicy.FULL,
    )

    assert event.source is IngressSource.SYNC
    assert event.kind is ChangeKind.CREATE
    assert event.raw_event_type == "create"
    assert event.payload_for_storage()["cursor"] == "cursor-1"


def test_delete_change_cannot_carry_an_item() -> None:
    with pytest.raises(IngressValidationError):
        validate_sync_change_contract(
            {"change_type": "delete", "id": "message-1", "item": {}}
        )


def test_sync_batch_rejects_duplicate_event_identity() -> None:
    change = SyncChange(ChangeKind.CREATE, "message-1", {"id": "message-1"})
    with pytest.raises(ValueError, match="duplicate"):
        SyncBatch("exchange_sync_contract_v2", "cursor-1", (change, change), True)


def test_runtime_requires_processing_context_before_opening_a_pool() -> None:
    with pytest.raises(ValueError, match="processing_context"):
        build_ingestion_runtime(
            SimpleNamespace(DURABLE_INBOX_ENABLED=True, POLLING_ENABLED=False)
        )
    with pytest.raises(ValueError, match="polling requires durable"):
        build_ingestion_runtime(
            SimpleNamespace(DURABLE_INBOX_ENABLED=False, POLLING_ENABLED=True)
        )


async def _guarded_processor(**_kwargs: object) -> object:
    raise AssertionError("not called by constructor")


def test_legacy_processor_bridge_remains_explicitly_constructed() -> None:
    adapter = LegacyProcessingAdapter(
        object(), legacy_account_id=1, guarded_processor=_guarded_processor
    )

    assert adapter.legacy_account_id == 1
    assert LegacyProcessingFailed().safe_code == "legacy.processing_failed"
