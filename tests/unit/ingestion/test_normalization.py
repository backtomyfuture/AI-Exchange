from __future__ import annotations

import hashlib
import inspect
import json
import logging
import math
from datetime import UTC, datetime
from typing import Any

import pytest

from src.domain.errors import (
    ErrorKind,
    IngressValidationCode,
    IngressValidationError,
)
from src.ingestion.models import (
    ChangeKind,
    IngressSource,
    NormalizedIngressEvent,
    ProcessingPolicy,
    SyncChange,
)
from src.ingestion.normalization import (
    normalize_sync_change,
    normalize_webhook_event,
    validate_sync_change_contract,
)
from src.safety.input_limits import InputLimitExceeded


def _webhook_payload(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "account_id": 8,
        "event": "NewMailEvent",
        "event_type": "NewMailEvent",
        "timestamp": 1_752_384_245,
        "item_id": {"id": "message-1", "changekey": "version-1"},
        "parent_folder_id": {"id": "INBOX"},
        "watermark": "watermark-1",
        "metadata": {"unicode": "合成邮件"},
    }
    payload.update(overrides)
    return payload


def _raw(payload: Any) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _normalize_webhook(
    payload: dict[str, Any] | None = None,
    *,
    raw_body: bytes | None = None,
    header_event: str | None = None,
    processing_policy: ProcessingPolicy = ProcessingPolicy.FULL,
) -> NormalizedIngressEvent:
    body = _webhook_payload() if payload is None else payload
    return normalize_webhook_event(
        raw_body=_raw(body) if raw_body is None else raw_body,
        payload=body,
        processing_policy=processing_policy,
        header_event=header_event,
    )


def _normalize_sync(
    change: SyncChange | None = None,
    *,
    account_id: int = 8,
    folder: str = "INBOX",
    cursor: str = "cursor-1",
    processing_policy: ProcessingPolicy = ProcessingPolicy.FULL,
) -> NormalizedIngressEvent:
    return normalize_sync_change(
        account_id=account_id,
        folder=folder,
        cursor=cursor,
        change=change
        or SyncChange(
            kind=ChangeKind.CREATE,
            external_email_id="message-1",
            item={"id": "message-1", "is_read": False},
            source_version="version-1",
        ),
        processing_policy=processing_policy,
    )


def _unsafe_sync_change(
    *,
    external_email_id: object,
    item: object,
    source_version: object = None,
) -> SyncChange:
    change = object.__new__(SyncChange)
    object.__setattr__(change, "kind", ChangeKind.CREATE)
    object.__setattr__(change, "external_email_id", external_email_id)
    object.__setattr__(change, "item", item)
    object.__setattr__(change, "source_version", source_version)
    return change


def _sync_transport_item(**overrides: object) -> dict[str, object]:
    item: dict[str, object] = {
        "id": "message-1",
        "subject": "safe subject",
        "sender": "sender@example.com",
        "received_time": "2026-07-13T08:09:10",
        "is_read": False,
        "has_attachments": True,
    }
    item.update(overrides)
    return item


def test_ingestion_boundary_exports_normalizers() -> None:
    import src.ingestion as ingestion

    assert ingestion.normalize_webhook_event is normalize_webhook_event
    assert ingestion.normalize_sync_change is normalize_sync_change
    assert ingestion.validate_sync_change_contract is validate_sync_change_contract


def test_ingestion_boundary_exports_sync_contract_validator() -> None:
    import src.ingestion as ingestion

    assert callable(getattr(ingestion, "validate_sync_change_contract", None))


def test_normalizers_require_keyword_only_explicit_policy() -> None:
    webhook_signature = inspect.signature(normalize_webhook_event)
    sync_signature = inspect.signature(normalize_sync_change)

    assert all(
        parameter.kind is inspect.Parameter.KEYWORD_ONLY
        for parameter in webhook_signature.parameters.values()
    )
    assert all(
        sync_signature.parameters[name].kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
        for name in ("account_id", "folder", "cursor", "change")
    )
    assert (
        webhook_signature.parameters["processing_policy"].default
        is inspect.Parameter.empty
    )
    assert (
        sync_signature.parameters["processing_policy"].default
        is inspect.Parameter.empty
    )

    change = SyncChange(
        ChangeKind.CREATE,
        "message-1",
        {"id": "message-1"},
    )
    event = normalize_sync_change(
        8,
        "INBOX",
        "cursor-1",
        change,
        processing_policy=ProcessingPolicy.FULL,
    )
    assert event.external_email_id == "message-1"


def test_normalized_event_has_no_fail_open_policy_default() -> None:
    parameter = inspect.signature(NormalizedIngressEvent).parameters[
        "processing_policy"
    ]

    assert parameter.default is inspect.Parameter.empty


def test_ignored_policy_is_preserved_and_excluded_from_dedupe() -> None:
    full = _normalize_webhook(processing_policy=ProcessingPolicy.FULL)
    ignored = _normalize_webhook(processing_policy=ProcessingPolicy.IGNORED)

    assert ignored.processing_policy is ProcessingPolicy.IGNORED
    assert ignored.dedupe_key == full.dedupe_key


def test_invalid_policy_is_a_safe_validation_error() -> None:
    payload = _webhook_payload()

    with pytest.raises(IngressValidationError) as caught:
        normalize_webhook_event(
            raw_body=_raw(payload),
            payload=payload,
            processing_policy="full",  # type: ignore[arg-type]
        )

    assert caught.value.safe_code is IngressValidationCode.POLICY_INVALID


@pytest.mark.parametrize(
    ("raw_body", "payload"),
    [
        (b"[]", {}),
        (b"not-json", {}),
        (b"\xff", {}),
        (b'{"account_id":8,"account_id":9}', {"account_id": 9}),
        (b'{"outer":{"id":"a","id":"b"}}', {"outer": {"id": "b"}}),
        (b'{"account_id":NaN}', {"account_id": float("nan")}),
        (b'{"account_id":Infinity}', {"account_id": float("inf")}),
    ],
)
def test_webhook_rejects_ambiguous_or_nonstandard_signed_json(
    raw_body: bytes,
    payload: dict[str, Any],
) -> None:
    with pytest.raises(IngressValidationError) as caught:
        normalize_webhook_event(
            raw_body=raw_body,
            payload=payload,
            processing_policy=ProcessingPolicy.FULL,
        )

    assert caught.value.safe_code is IngressValidationCode.INVALID_BODY


def test_webhook_rejects_nonbytes_raw_body() -> None:
    payload = _webhook_payload()

    with pytest.raises(IngressValidationError) as caught:
        normalize_webhook_event(
            raw_body=bytearray(_raw(payload)),  # type: ignore[arg-type]
            payload=payload,
            processing_policy=ProcessingPolicy.FULL,
        )

    assert caught.value.safe_code is IngressValidationCode.INVALID_BODY


def test_webhook_wraps_overlong_json_integer_as_safe_invalid_body() -> None:
    raw_body = b'{"account_id":' + (b"9" * 5000) + b"}"

    with pytest.raises(IngressValidationError) as caught:
        normalize_webhook_event(
            raw_body=raw_body,
            payload={},
            processing_policy=ProcessingPolicy.FULL,
        )

    assert caught.value.safe_code is IngressValidationCode.INVALID_BODY
    assert caught.value.__cause__ is None


def test_webhook_signed_body_is_authoritative_and_must_match_payload() -> None:
    signed = _webhook_payload()
    supplied = {**signed, "account_id": 9}

    with pytest.raises(IngressValidationError) as caught:
        normalize_webhook_event(
            raw_body=_raw(signed),
            payload=supplied,
            processing_policy=ProcessingPolicy.FULL,
        )

    assert caught.value.safe_code is IngressValidationCode.BODY_PAYLOAD_MISMATCH


@pytest.mark.parametrize(
    "payload",
    [None, [], "not-an-object"],
)
def test_webhook_rejects_nonmapping_supplied_payload(payload: object) -> None:
    signed = _webhook_payload()

    with pytest.raises(IngressValidationError) as caught:
        normalize_webhook_event(
            raw_body=_raw(signed),
            payload=payload,  # type: ignore[arg-type]
            processing_policy=ProcessingPolicy.FULL,
        )

    assert caught.value.safe_code is IngressValidationCode.BODY_PAYLOAD_MISMATCH


@pytest.mark.parametrize(
    "invalid_value",
    [{"set-value"}, object(), float("nan")],
)
def test_webhook_rejects_non_json_supplied_payload_values(
    invalid_value: object,
) -> None:
    signed = _webhook_payload()
    supplied = {**signed, "invalid": invalid_value}

    with pytest.raises(IngressValidationError) as caught:
        normalize_webhook_event(
            raw_body=_raw(signed),
            payload=supplied,
            processing_policy=ProcessingPolicy.FULL,
        )

    assert caught.value.safe_code is IngressValidationCode.BODY_PAYLOAD_MISMATCH


def test_webhook_safely_rejects_hostile_supplied_mapping() -> None:
    sentinel = "TOP-SECRET-WEBHOOK-MAPPING"
    signed = _webhook_payload()

    class HostileMapping(dict[str, object]):
        def items(self):  # type: ignore[override]
            raise RuntimeError(sentinel)

    with pytest.raises(IngressValidationError) as caught:
        normalize_webhook_event(
            raw_body=_raw(signed),
            payload=HostileMapping(signed),
            processing_policy=ProcessingPolicy.FULL,
        )

    assert caught.value.safe_code is IngressValidationCode.BODY_PAYLOAD_MISMATCH
    assert caught.value.__cause__ is None
    assert sentinel not in str(caught.value)
    assert sentinel not in repr(caught.value)


def test_webhook_rejects_cyclic_supplied_payload_containers() -> None:
    signed = _webhook_payload()
    mapping_cycle: dict[str, object] = {}
    mapping_cycle["self"] = mapping_cycle
    sequence_cycle: list[object] = []
    sequence_cycle.append(sequence_cycle)

    for cycle in (mapping_cycle, sequence_cycle):
        supplied = {**signed, "cycle": cycle}
        with pytest.raises(IngressValidationError) as caught:
            normalize_webhook_event(
                raw_body=_raw(signed),
                payload=supplied,
                processing_policy=ProcessingPolicy.FULL,
            )
        assert caught.value.safe_code is IngressValidationCode.BODY_PAYLOAD_MISMATCH


def test_webhook_rejects_nonstring_json_object_keys_in_supplied_payload() -> None:
    signed = _webhook_payload()
    supplied: dict[object, object] = dict(signed)
    supplied[1] = "invalid-key"

    with pytest.raises(IngressValidationError) as caught:
        normalize_webhook_event(
            raw_body=_raw(signed),
            payload=supplied,  # type: ignore[arg-type]
            processing_policy=ProcessingPolicy.FULL,
        )

    assert caught.value.safe_code is IngressValidationCode.BODY_PAYLOAD_MISMATCH


@pytest.mark.parametrize(
    "extra",
    [
        {"subject": "private-sentinel\x00suffix"},
        {"private-sentinel\x00key": "value"},
        {"nested": [{"subject": "private-sentinel\x00suffix"}]},
    ],
)
def test_webhook_rejects_jsonb_unrepresentable_nul_without_leak(
    extra: dict[str, object],
) -> None:
    payload = _webhook_payload(extra=extra)

    with pytest.raises(IngressValidationError) as caught:
        _normalize_webhook(payload)

    error = caught.value
    assert error.safe_code is IngressValidationCode.INVALID_BODY
    assert "private-sentinel" not in str(error)
    assert "private-sentinel" not in repr(error)
    assert error.__cause__ is None


def test_webhook_rejects_ascii_escaped_lone_surrogate_without_leak() -> None:
    payload = _webhook_payload()
    raw_body = _raw(payload).replace(
        "合成邮件".encode("utf-8"),
        b"\\ud800",
    )

    with pytest.raises(IngressValidationError) as caught:
        _normalize_webhook(payload, raw_body=raw_body)

    assert caught.value.safe_code is IngressValidationCode.INVALID_BODY
    assert caught.value.__cause__ is None


def test_webhook_preserves_the_full_validated_signed_payload() -> None:
    payload = _webhook_payload(extra={"nested": [1, "二", True], "finite_ratio": 0.5})

    event = _normalize_webhook(payload)

    assert event.payload["extra"]["nested"] == (1, "二", True)
    assert event.payload["metadata"]["unicode"] == "合成邮件"
    assert event.payload["extra"]["finite_ratio"] == 0.5


def test_webhook_wraps_oversized_normalized_payload_with_safe_error() -> None:
    payload = _webhook_payload(oversized="x" * (256 * 1024))

    with pytest.raises(IngressValidationError) as caught:
        _normalize_webhook(payload)

    assert caught.value.safe_code is IngressValidationCode.NORMALIZED_EVENT_INVALID


def test_webhook_rejects_postgres_jsonb_numeric_expansion_with_safe_error() -> None:
    payload = _webhook_payload(metadata=[1e-300] * 1_000)

    with pytest.raises(IngressValidationError) as caught:
        _normalize_webhook(payload)

    assert caught.value.safe_code is IngressValidationCode.NORMALIZED_EVENT_INVALID
    assert caught.value.__cause__ is None


@pytest.mark.parametrize(
    ("raw_event_type", "kind"),
    [
        ("NewMailEvent", ChangeKind.CREATE),
        ("CreatedEvent", ChangeKind.CREATE),
        ("ModifiedEvent", ChangeKind.UPDATE),
        ("DeletedEvent", ChangeKind.DELETE),
    ],
)
def test_webhook_maps_only_supported_exact_event_names(
    raw_event_type: str,
    kind: ChangeKind,
) -> None:
    event = _normalize_webhook(
        _webhook_payload(event=raw_event_type, event_type=raw_event_type)
    )

    assert event.raw_event_type == raw_event_type
    assert event.kind is kind


@pytest.mark.parametrize(
    "raw_event_type",
    ["MovedEvent", "CopiedEvent", "FreeBusyChangedEvent", "newmail"],
)
def test_webhook_rejects_unsupported_or_aliased_event_names(
    raw_event_type: str,
) -> None:
    with pytest.raises(IngressValidationError) as caught:
        _normalize_webhook(
            _webhook_payload(event=raw_event_type, event_type=raw_event_type)
        )

    assert caught.value.safe_code is IngressValidationCode.EVENT_UNSUPPORTED


def test_webhook_requires_body_event_even_when_header_is_present() -> None:
    payload = _webhook_payload()
    payload.pop("event")
    payload.pop("event_type")

    with pytest.raises(IngressValidationError) as caught:
        _normalize_webhook(payload, header_event="NewMailEvent")

    assert caught.value.safe_code is IngressValidationCode.EVENT_MISSING


def test_webhook_rejects_conflicting_body_event_fields() -> None:
    with pytest.raises(IngressValidationError) as caught:
        _normalize_webhook(_webhook_payload(event_type="ModifiedEvent"))

    assert caught.value.safe_code is IngressValidationCode.EVENT_CONFLICT


def test_webhook_event_names_are_exact_and_null_cannot_mask_conflict() -> None:
    with pytest.raises(IngressValidationError) as whitespace:
        _normalize_webhook(
            _webhook_payload(
                event=" NewMailEvent ",
                event_type=" NewMailEvent ",
            )
        )
    assert whitespace.value.safe_code is IngressValidationCode.EVENT_UNSUPPORTED

    with pytest.raises(IngressValidationError) as null_conflict:
        _normalize_webhook(_webhook_payload(event_type=None))
    assert null_conflict.value.safe_code is IngressValidationCode.EVENT_CONFLICT


@pytest.mark.parametrize("remaining_field", ["event", "event_type"])
def test_webhook_accepts_either_single_signed_event_field(
    remaining_field: str,
) -> None:
    payload = _webhook_payload()
    payload.pop("event_type" if remaining_field == "event" else "event")

    event = _normalize_webhook(payload)

    assert event.raw_event_type == "NewMailEvent"


def test_webhook_rejects_single_null_signed_event_field_as_missing() -> None:
    payload = _webhook_payload()
    payload.pop("event_type")
    payload["event"] = None

    with pytest.raises(IngressValidationError) as caught:
        _normalize_webhook(payload)

    assert caught.value.safe_code is IngressValidationCode.EVENT_MISSING


def test_webhook_header_is_optional_consistency_assertion_only() -> None:
    absent = _normalize_webhook()
    matching = _normalize_webhook(header_event="NewMailEvent")

    assert matching.dedupe_key == absent.dedupe_key

    with pytest.raises(IngressValidationError) as caught:
        _normalize_webhook(header_event="ModifiedEvent")

    assert caught.value.safe_code is IngressValidationCode.HEADER_EVENT_MISMATCH


@pytest.mark.parametrize("account_id", [0, -1, True, "8", None, 2**63, 2**100])
def test_webhook_rejects_invalid_body_account(account_id: object) -> None:
    with pytest.raises(IngressValidationError) as caught:
        _normalize_webhook(_webhook_payload(account_id=account_id))

    assert caught.value.safe_code is IngressValidationCode.ACCOUNT_INVALID


def test_webhook_email_id_priority_and_consistency() -> None:
    event = _normalize_webhook(
        _webhook_payload(
            id="message-1",
            item={"id": "message-1"},
        )
    )
    assert event.external_email_id == "message-1"

    fallback = _webhook_payload(id="message-top")
    fallback.pop("item_id")
    assert _normalize_webhook(fallback).external_email_id == "message-top"


def test_webhook_rejects_missing_email_id_and_nonstring_header() -> None:
    missing = _webhook_payload()
    missing.pop("item_id")

    with pytest.raises(IngressValidationError) as absent:
        _normalize_webhook(missing)
    assert absent.value.safe_code is IngressValidationCode.EMAIL_ID_INVALID

    with pytest.raises(IngressValidationError) as header:
        _normalize_webhook(header_event=42)  # type: ignore[arg-type]
    assert header.value.safe_code is IngressValidationCode.HEADER_EVENT_MISMATCH


def test_webhook_rejects_conflicting_or_malformed_high_priority_email_id() -> None:
    with pytest.raises(IngressValidationError) as conflict:
        _normalize_webhook(_webhook_payload(id="different-message"))
    assert conflict.value.safe_code is IngressValidationCode.EMAIL_ID_CONFLICT

    with pytest.raises(IngressValidationError) as malformed:
        _normalize_webhook(
            _webhook_payload(item_id="ItemId(id='secret-message')", id="message-1")
        )
    assert malformed.value.safe_code is IngressValidationCode.EMAIL_ID_INVALID


@pytest.mark.parametrize(
    ("payload", "code"),
    [
        (
            _webhook_payload(item_id={"id": " message-1 ", "changekey": "version-1"}),
            IngressValidationCode.EMAIL_ID_INVALID,
        ),
        (
            _webhook_payload(item_id={"id": "message-1", "changekey": " version-1 "}),
            IngressValidationCode.VERSION_INVALID,
        ),
    ],
)
def test_webhook_rejects_whitespace_normalization_of_opaque_tokens(
    payload: dict[str, Any],
    code: IngressValidationCode,
) -> None:
    with pytest.raises(IngressValidationError) as caught:
        _normalize_webhook(payload)

    assert caught.value.safe_code is code


def test_webhook_parent_folder_is_authoritative_and_required() -> None:
    missing = _webhook_payload(folder="INBOX")
    missing.pop("parent_folder_id")

    with pytest.raises(IngressValidationError) as caught:
        _normalize_webhook(missing)

    assert caught.value.safe_code is IngressValidationCode.FOLDER_INVALID

    with pytest.raises(IngressValidationError) as missing_id:
        _normalize_webhook(_webhook_payload(parent_folder_id={}))
    assert missing_id.value.safe_code is IngressValidationCode.FOLDER_INVALID


def test_webhook_folder_assertions_must_match_parent_folder() -> None:
    matching = _normalize_webhook(
        _webhook_payload(
            folder="INBOX",
            folder_id={"id": "INBOX"},
            item={"parent_folder_id": {"id": "INBOX"}},
        )
    )
    assert matching.folder == "INBOX"

    with pytest.raises(IngressValidationError) as caught:
        _normalize_webhook(_webhook_payload(item={"parent_folder_id": {"id": "OTHER"}}))
    assert caught.value.safe_code is IngressValidationCode.FOLDER_CONFLICT


def test_webhook_changekey_priority_conflict_and_watermark_fallback() -> None:
    consistent = _normalize_webhook(
        _webhook_payload(
            changekey="version-1",
            item={"changekey": "version-1"},
            watermark="different-delivery-watermark",
        )
    )
    assert consistent.source_version == "version-1"

    with pytest.raises(IngressValidationError) as caught:
        _normalize_webhook(_webhook_payload(changekey="version-2"))
    assert caught.value.safe_code is IngressValidationCode.VERSION_CONFLICT

    fallback = _webhook_payload(watermark="watermark-only")
    fallback["item_id"].pop("changekey")
    assert _normalize_webhook(fallback).source_version == "watermark-only"


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (1_752_384_245, datetime.fromtimestamp(1_752_384_245, UTC)),
        (1_752_384_245.5, datetime.fromtimestamp(1_752_384_245.5, UTC)),
        ("2026-07-13T08:09:10Z", datetime(2026, 7, 13, 8, 9, 10, tzinfo=UTC)),
        (
            "2026-07-13T16:09:10+08:00",
            datetime(2026, 7, 13, 8, 9, 10, tzinfo=UTC),
        ),
    ],
)
def test_webhook_normalizes_trusted_body_timestamp(
    value: object,
    expected: datetime,
) -> None:
    event = _normalize_webhook(_webhook_payload(timestamp=value))

    assert event.source_event_at == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("0002-01-01T00:00:00Z", datetime(2, 1, 1, tzinfo=UTC)),
        (
            "0002-01-01T00:00:00.000001Z",
            datetime(2, 1, 1, 0, 0, 0, 1, tzinfo=UTC),
        ),
        (
            "9998-12-31T23:59:59.999999Z",
            datetime(9998, 12, 31, 23, 59, 59, 999999, tzinfo=UTC),
        ),
        ("0002-01-01T08:00:00+08:00", datetime(2, 1, 1, tzinfo=UTC)),
        (
            "9999-01-01T07:59:59.999999+08:00",
            datetime(9998, 12, 31, 23, 59, 59, 999999, tzinfo=UTC),
        ),
    ],
)
def test_webhook_accepts_source_event_at_inside_utc_database_window(
    value: object,
    expected: datetime,
) -> None:
    event = _normalize_webhook(_webhook_payload(timestamp=value))

    assert event.source_event_at == expected


@pytest.mark.parametrize(
    "value",
    [
        "0001-12-31T23:59:59.999999Z",
        "9999-01-01T00:00:00Z",
        "9999-01-01T00:00:00.000001Z",
        "0002-01-01T07:59:59.999999+08:00",
        "9999-01-01T08:00:00+08:00",
    ],
)
def test_webhook_rejects_iso_source_event_at_outside_utc_database_window(
    value: str,
) -> None:
    with pytest.raises(IngressValidationError) as caught:
        _normalize_webhook(_webhook_payload(timestamp=value))

    assert caught.value.safe_code is IngressValidationCode.TIMESTAMP_INVALID


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (-62_104_060_800, datetime(2, 1, 1, tzinfo=UTC)),
        (253_370_764_799, datetime(9998, 12, 31, 23, 59, 59, tzinfo=UTC)),
        (-62_104_060_800.0, datetime(2, 1, 1, tzinfo=UTC)),
        (
            math.nextafter(253_370_764_800.0, -math.inf),
            datetime(9998, 12, 31, 23, 59, 59, 999969, tzinfo=UTC),
        ),
    ],
)
def test_webhook_accepts_last_safe_numeric_source_event_at(
    value: int | float,
    expected: datetime,
) -> None:
    event = _normalize_webhook(_webhook_payload(timestamp=value))

    assert event.source_event_at == expected


@pytest.mark.parametrize(
    "value",
    [
        -62_104_060_801,
        253_370_764_800,
        math.nextafter(-62_104_060_800.0, -math.inf),
        253_370_764_800.0,
    ],
)
def test_webhook_rejects_first_unsafe_numeric_source_event_at(
    value: int | float,
) -> None:
    with pytest.raises(IngressValidationError) as caught:
        _normalize_webhook(_webhook_payload(timestamp=value))

    assert caught.value.safe_code is IngressValidationCode.TIMESTAMP_INVALID


def test_webhook_preserves_bool_and_nan_timestamp_rejection_semantics() -> None:
    with pytest.raises(IngressValidationError) as bool_caught:
        _normalize_webhook(_webhook_payload(timestamp=True))
    assert bool_caught.value.safe_code is IngressValidationCode.TIMESTAMP_INVALID

    nan_payload = _webhook_payload(timestamp=float("nan"))
    with pytest.raises(IngressValidationError) as nan_caught:
        _normalize_webhook(
            nan_payload,
            raw_body=json.dumps(nan_payload, allow_nan=True).encode("utf-8"),
        )
    assert nan_caught.value.safe_code is IngressValidationCode.INVALID_BODY


@pytest.mark.parametrize(
    "value",
    [
        "2026-07-13T08:09:10",
        "not-a-timestamp",
        "0001-01-01T00:00:00+14:00",
        "9999-12-31T23:59:59-14:00",
        True,
        {"unexpected": "type"},
        float("nan"),
        float("inf"),
        10**30,
        10**500,
    ],
)
def test_webhook_rejects_invalid_body_timestamp(value: object) -> None:
    payload = _webhook_payload(timestamp=value)
    raw_body = (
        _raw(payload)
        if not isinstance(value, float) or value == value and value != float("inf")
        else json.dumps(payload, allow_nan=True).encode("utf-8")
    )

    with pytest.raises(IngressValidationError) as caught:
        _normalize_webhook(payload, raw_body=raw_body)

    assert caught.value.safe_code in {
        IngressValidationCode.INVALID_BODY,
        IngressValidationCode.TIMESTAMP_INVALID,
    }


def test_webhook_allows_missing_timestamp_but_never_uses_a_header_timestamp() -> None:
    payload = _webhook_payload()
    payload.pop("timestamp")

    event = _normalize_webhook(payload)

    assert event.source_event_at is None


def test_versioned_webhook_dedupe_ignores_delivery_time_body_and_policy() -> None:
    first = _normalize_webhook()
    retried_payload = _webhook_payload(
        timestamp="2026-07-13T08:09:10Z",
        retry_metadata={"attempt": 2},
    )
    retried = _normalize_webhook(
        retried_payload,
        processing_policy=ProcessingPolicy.ARCHIVE,
        header_event="NewMailEvent",
    )

    assert retried.dedupe_key == first.dedupe_key


def test_unversioned_webhook_dedupe_prefers_trusted_time_over_raw_body() -> None:
    first_payload = _webhook_payload()
    first_payload["item_id"].pop("changekey")
    first_payload.pop("watermark")
    second_payload = {**first_payload, "delivery_attempt": 2}

    first = _normalize_webhook(first_payload)
    exact_retry = _normalize_webhook(first_payload)
    changed_body = _normalize_webhook(second_payload)

    assert exact_retry.dedupe_key == first.dedupe_key
    assert changed_body.dedupe_key == first.dedupe_key
    assert (
        first.dedupe_key
        == "c6e64dac21343eefd0edd97504fa5ffcb365b6646831bf104049e735f5d3b300"
    )


def test_unversioned_untimed_webhook_dedupe_falls_back_to_exact_raw_body() -> None:
    first_payload = _webhook_payload()
    first_payload["item_id"].pop("changekey")
    first_payload.pop("watermark")
    first_payload.pop("timestamp")
    second_payload = {**first_payload, "delivery_attempt": 2}

    first = _normalize_webhook(first_payload)
    exact_retry = _normalize_webhook(first_payload)
    changed_body = _normalize_webhook(second_payload)

    assert exact_retry.dedupe_key == first.dedupe_key
    assert changed_body.dedupe_key != first.dedupe_key
    assert (
        first.dedupe_key
        == "e5496e2b5c7a8a229c2b4cffdec7c56815a4184ec368ca0cb1b33519b9d4f073"
    )


def test_webhook_dedupe_uses_locked_canonical_identity() -> None:
    event = _normalize_webhook()
    identity = {
        "schema_version": 1,
        "account_id": 8,
        "source": "webhook",
        "raw_event_type": "NewMailEvent",
        "kind": "create",
        "external_email_id": "message-1",
        "folder": "INBOX",
        "source_version": "version-1",
        "cursor": None,
        "source_event_at": None,
        "raw_body_sha256": None,
    }
    canonical = json.dumps(
        identity,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")

    assert event.dedupe_key == hashlib.sha256(canonical).hexdigest()
    assert (
        event.dedupe_key
        == "042482c5a95328fb13bcbb22c8223d661c11ecf5f4e25732108b106d32ba8ade"
    )


@pytest.mark.parametrize(
    ("kind", "item"),
    [
        (ChangeKind.CREATE, {"id": "message-1", "is_read": False}),
        (ChangeKind.UPDATE, {"id": "message-1", "is_read": True}),
        (ChangeKind.DELETE, None),
    ],
)
def test_sync_normalizes_supported_change_shapes(
    kind: ChangeKind,
    item: dict[str, Any] | None,
) -> None:
    change = SyncChange(
        kind=kind,
        external_email_id="message-1",
        item=item,
        source_version="version-1",
    )

    event = _normalize_sync(change)

    assert event.source is IngressSource.SYNC
    assert event.raw_event_type == kind.value
    assert event.kind is kind
    assert event.source_version == "version-1"
    assert event.source_event_at is None
    assert event.payload == {
        "cursor": "cursor-1",
        "change_type": kind.value,
        "id": "message-1",
        "item": item,
        "source_version": "version-1",
    }


def test_sync_read_state_remains_update_and_received_time_is_not_trusted() -> None:
    change = SyncChange(
        kind=ChangeKind.UPDATE,
        external_email_id="message-1",
        item={
            "id": "message-1",
            "is_read": True,
            "received_time": "2026-07-13T08:09:10",
        },
    )

    event = _normalize_sync(change)

    assert event.kind is ChangeKind.UPDATE
    assert event.source_event_at is None


def test_sync_contract_validator_returns_a_clean_typed_change() -> None:
    validated = validate_sync_change_contract(
        SyncChange(
            ChangeKind.CREATE,
            "message-1",
            {"id": "message-1", "subject": "safe subject", "is_read": False},
            "version-1",
        )
    )

    assert type(validated) is SyncChange
    assert validated.kind is ChangeKind.CREATE
    assert validated.external_email_id == "message-1"
    assert validated.source_version == "version-1"
    assert validated.item == {
        "id": "message-1",
        "subject": "safe subject",
        "is_read": False,
    }


def test_sync_contract_validator_builds_change_from_transport_mapping() -> None:
    validated = validate_sync_change_contract(
        {
            "change_type": "create",
            "id": "message-1",
            "item": _sync_transport_item(),
        }
    )

    assert validated.kind is ChangeKind.CREATE
    assert validated.external_email_id == "message-1"
    assert validated.source_version is None
    assert validated.item is not None
    assert validated.item["has_attachments"] is True


@pytest.mark.parametrize(
    "external_email_id", ["message\x1f", "message\x7f", "message\x80"]
)
def test_sync_contract_validator_rejects_c0_c1_transport_identifiers(
    external_email_id: str,
) -> None:
    with pytest.raises(IngressValidationError) as caught:
        validate_sync_change_contract(
            {
                "change_type": "create",
                "id": external_email_id,
                "item": _sync_transport_item(id=external_email_id),
            }
        )

    assert caught.value.safe_code is IngressValidationCode.EMAIL_ID_INVALID


@pytest.mark.parametrize(
    "received_time",
    [
        "2026-07-13",
        "2026-W29-1T08:09:10",
        "2026-07-13 08:09:10",
        "2026-07-13_08:09:10",
        "2026-07-13T08:09:10.123456",
        "2026-07-13T08:09:10+08:00",
        "2026-02-30T08:09:10",
    ],
)
def test_sync_transport_received_time_matches_exact_service_wire_format(
    received_time: str,
) -> None:
    with pytest.raises(IngressValidationError) as caught:
        validate_sync_change_contract(
            {
                "change_type": "create",
                "id": "message-1",
                "item": _sync_transport_item(received_time=received_time),
            }
        )

    assert caught.value.safe_code is IngressValidationCode.SYNC_ITEM_INVALID


def test_sync_contract_validator_allows_missing_inner_item_id() -> None:
    validated = validate_sync_change_contract(
        SyncChange(
            ChangeKind.CREATE,
            "message-1",
            {"subject": "safe subject"},
        )
    )

    assert validated.external_email_id == "message-1"
    assert validated.item == {"subject": "safe subject"}


@pytest.mark.parametrize(
    "change",
    [
        SyncChange(ChangeKind.READ, "message-1", {"id": "message-1"}),
        SyncChange(ChangeKind.CREATE, "message-1", None),
        SyncChange(ChangeKind.UPDATE, "message-1", None),
        SyncChange(ChangeKind.DELETE, "message-1", {"id": "message-1"}),
    ],
)
def test_sync_rejects_unsupported_or_inconsistent_item_shapes(
    change: SyncChange,
) -> None:
    with pytest.raises(IngressValidationError) as caught:
        _normalize_sync(change)

    assert caught.value.safe_code in {
        IngressValidationCode.SYNC_CHANGE_INVALID,
        IngressValidationCode.SYNC_ITEM_INVALID,
    }


@pytest.mark.parametrize(
    "change",
    [
        SyncChange(ChangeKind.READ, "message-1", {"id": "message-1"}),
        SyncChange(ChangeKind.CREATE, "message-1", None),
        SyncChange(ChangeKind.UPDATE, "message-1", None),
        SyncChange(ChangeKind.DELETE, "message-1", {"id": "message-1"}),
        SyncChange(ChangeKind.CREATE, "message-1", {"id": None}),
        SyncChange(ChangeKind.CREATE, " message-1 ", {"id": " message-1 "}),
        SyncChange(
            ChangeKind.CREATE,
            "message-1",
            {"id": "message-1"},
            " version-1 ",
        ),
        SyncChange(ChangeKind.CREATE, "outer-message", {"id": "inner-message"}),
    ],
)
def test_sync_contract_validator_rejects_invalid_transport_change(
    change: SyncChange,
) -> None:
    with pytest.raises(IngressValidationError):
        validate_sync_change_contract(change)


@pytest.mark.parametrize(
    "change",
    [
        {"change_type": "create", "id": "message-1"},
        {
            "change_type": 1,
            "id": "message-1",
            "item": _sync_transport_item(),
        },
        {
            "change_type": "create",
            "id": "message-1",
            "item": _sync_transport_item(),
            "unexpected": True,
        },
        {
            "change_type": "create",
            "id": "message-1",
            "item": _sync_transport_item(),
            "source_version": "version-1",
        },
        {
            "change_type": "update",
            "id": "message-1",
            "item": _sync_transport_item(subject={"private": "value"}),
        },
        {
            "change_type": "update",
            "id": "message-1",
            "item": _sync_transport_item(is_read="yes"),
        },
        {
            "change_type": "update",
            "id": "message-1",
            "item": _sync_transport_item(is_read=None),
        },
        {
            "change_type": "update",
            "id": "message-1",
            "item": _sync_transport_item(id=None),
        },
        {
            "change_type": "update",
            "id": "message-1",
            "item": _sync_transport_item(received_time="not-a-time"),
        },
        {
            "change_type": "create",
            "id": "message-1",
            "item": {"id": "message-1", "subject": "incomplete"},
        },
        {
            "change_type": "create",
            "id": "outer-message",
            "item": _sync_transport_item(id="inner-message"),
        },
    ],
)
def test_sync_contract_validator_rejects_invalid_transport_mapping(
    change: dict[str, object],
) -> None:
    with pytest.raises(IngressValidationError):
        validate_sync_change_contract(change)


def test_sync_contract_validator_safely_rejects_hostile_mapping() -> None:
    sentinel = "TOP-SECRET-SYNC-MAPPING"

    class HostileMapping(dict[str, object]):
        inspected = False

        def items(self):  # type: ignore[override]
            self.inspected = True
            raise RuntimeError(sentinel)

    change = HostileMapping(
        change_type="create",
        id="message-1",
        item={"id": "message-1"},
    )

    with pytest.raises(IngressValidationError) as caught:
        validate_sync_change_contract(change)

    assert change.inspected is True
    assert caught.value.safe_code is IngressValidationCode.SYNC_CHANGE_INVALID
    assert caught.value.__cause__ is None
    assert sentinel not in str(caught.value)
    assert sentinel not in repr(caught.value)


def test_sync_contract_validator_rejects_non_change_object() -> None:
    with pytest.raises(IngressValidationError) as caught:
        validate_sync_change_contract(object())  # type: ignore[arg-type]

    assert caught.value.safe_code is IngressValidationCode.SYNC_CHANGE_INVALID


@pytest.mark.parametrize(
    ("field", "expected_code"),
    [
        ("external_email_id", IngressValidationCode.EMAIL_ID_INVALID),
        ("source_version", IngressValidationCode.VERSION_INVALID),
    ],
)
def test_sync_contract_validator_safely_rejects_hostile_token_protocol(
    field: str,
    expected_code: IngressValidationCode,
) -> None:
    sentinel = "TOP-SECRET-SYNC-TOKEN"

    class HostileText(str):
        def strip(self, _chars: str | None = None) -> str:
            raise RuntimeError(sentinel)

    change = _unsafe_sync_change(
        external_email_id="message-1",
        item={"id": "message-1"},
        source_version=None,
    )
    object.__setattr__(change, field, HostileText("opaque"))

    with pytest.raises(IngressValidationError) as caught:
        validate_sync_change_contract(change)

    assert caught.value.safe_code is expected_code
    assert caught.value.__cause__ is None
    assert sentinel not in str(caught.value)


@pytest.mark.parametrize(
    ("kind", "item", "expected_code"),
    [
        ([], {"id": "message-1"}, IngressValidationCode.SYNC_CHANGE_INVALID),
        ("create", {"id": "message-1"}, IngressValidationCode.SYNC_CHANGE_INVALID),
        (ChangeKind.CREATE, 0, IngressValidationCode.SYNC_ITEM_INVALID),
    ],
)
def test_sync_wraps_corrupted_typed_change_fields_as_safe_validation_errors(
    kind: object,
    item: object,
    expected_code: IngressValidationCode,
) -> None:
    change = _unsafe_sync_change(external_email_id="message-1", item=item)
    object.__setattr__(change, "kind", kind)

    with pytest.raises(IngressValidationError) as caught:
        _normalize_sync(change)

    assert caught.value.safe_code is expected_code
    assert caught.value.__cause__ is None


@pytest.mark.parametrize("error_type", [AttributeError, RuntimeError])
def test_sync_suppresses_sensitive_protocol_error_context(
    error_type: type[Exception],
) -> None:
    sentinel = "TOP-SECRET-SYNC-ATTRIBUTE"

    class CorruptedSyncChange(SyncChange):
        def __getattribute__(self, name: str) -> object:
            if name == "kind":
                raise error_type(sentinel)
            return super().__getattribute__(name)

    change = object.__new__(CorruptedSyncChange)

    with pytest.raises(IngressValidationError) as caught:
        _normalize_sync(change)

    assert caught.value.safe_code is IngressValidationCode.SYNC_CHANGE_INVALID
    assert caught.value.__suppress_context__ is True
    assert sentinel not in str(caught.value)
    assert sentinel not in repr(caught.value)


def test_sync_rejects_inner_outer_id_conflict() -> None:
    change = SyncChange(
        ChangeKind.CREATE,
        "outer-message",
        {"id": "inner-message"},
    )

    with pytest.raises(IngressValidationError) as caught:
        _normalize_sync(change)

    assert caught.value.safe_code is IngressValidationCode.SYNC_ITEM_ID_CONFLICT


def test_sync_wraps_jsonb_unrepresentable_nul_without_leak() -> None:
    change = object.__new__(SyncChange)
    object.__setattr__(change, "kind", ChangeKind.CREATE)
    object.__setattr__(change, "external_email_id", "message-1")
    object.__setattr__(
        change,
        "item",
        {"id": "message-1", "subject": "private-sentinel\x00suffix"},
    )
    object.__setattr__(change, "source_version", None)

    with pytest.raises(IngressValidationError) as caught:
        _normalize_sync(change)

    error = caught.value
    assert error.safe_code is IngressValidationCode.SYNC_ITEM_INVALID
    assert "private-sentinel" not in str(error)
    assert "private-sentinel" not in repr(error)
    assert error.__cause__ is None


def test_sync_wraps_nested_lone_surrogate_as_item_validation_error() -> None:
    change = _unsafe_sync_change(
        external_email_id="message-1",
        item={"id": "message-1", "nested": [{"subject": "\ud800"}]},
    )

    with pytest.raises(IngressValidationError) as caught:
        _normalize_sync(change)

    assert caught.value.safe_code is IngressValidationCode.SYNC_ITEM_INVALID
    assert caught.value.__cause__ is None


def test_sync_rejects_unknown_numeric_item_field_with_safe_error() -> None:
    change = object.__new__(SyncChange)
    object.__setattr__(change, "kind", ChangeKind.CREATE)
    object.__setattr__(change, "external_email_id", "message-1")
    object.__setattr__(
        change,
        "item",
        {"id": "message-1", "metadata": [1e-300] * 1_000},
    )
    object.__setattr__(change, "source_version", None)

    with pytest.raises(IngressValidationError) as caught:
        _normalize_sync(change)

    assert caught.value.safe_code is IngressValidationCode.SYNC_ITEM_INVALID
    assert caught.value.__cause__ is None


def test_sync_folder_aliases_are_uppercase_but_custom_case_is_preserved() -> None:
    upper = _normalize_sync(folder="INBOX")
    lower = _normalize_sync(folder=" inbox ")
    custom = _normalize_sync(folder=" Team Box ")
    custom_case = _normalize_sync(folder="team box")

    assert lower.folder == "INBOX"
    assert lower.dedupe_key == upper.dedupe_key
    assert custom.folder == "Team Box"
    assert custom_case.folder == "team box"
    assert custom.dedupe_key != custom_case.dedupe_key

    sent_aliases = [
        _normalize_sync(folder=value)
        for value in ("SENT", "sent", "SentItems", "sent items")
    ]
    assert {event.folder for event in sent_aliases} == {"SENT"}
    assert len({event.dedupe_key for event in sent_aliases}) == 1


@pytest.mark.parametrize(
    ("custom", "standard"),
    [
        ("s_e_n_t", "SENT"),
        ("i n b o x", "INBOX"),
        ("j-u_n_k", "JUNK"),
        ("ſent", "SENT"),
    ],
)
def test_sync_custom_folder_separators_never_collapse_into_standard_alias(
    custom: str,
    standard: str,
) -> None:
    custom_event = _normalize_sync(folder=custom)
    standard_event = _normalize_sync(folder=standard)

    assert custom_event.folder == custom
    assert custom_event.dedupe_key != standard_event.dedupe_key


@pytest.mark.parametrize(
    ("change", "cursor", "code"),
    [
        (
            SyncChange(
                ChangeKind.CREATE,
                " message-1 ",
                {"id": " message-1 "},
                "version-1",
            ),
            "cursor-1",
            IngressValidationCode.EMAIL_ID_INVALID,
        ),
        (
            _unsafe_sync_change(
                external_email_id="\ud800",
                item={},
                source_version="version-1",
            ),
            "cursor-1",
            IngressValidationCode.EMAIL_ID_INVALID,
        ),
        (
            SyncChange(
                ChangeKind.CREATE,
                "message-1",
                {"id": "message-1"},
                " version-1 ",
            ),
            "cursor-1",
            IngressValidationCode.VERSION_INVALID,
        ),
        (
            SyncChange(
                ChangeKind.CREATE,
                "message-1",
                {"id": "message-1"},
                "version-1",
            ),
            " cursor-1 ",
            IngressValidationCode.CURSOR_INVALID,
        ),
    ],
)
def test_sync_rejects_whitespace_normalization_of_opaque_tokens(
    change: SyncChange,
    cursor: str,
    code: IngressValidationCode,
) -> None:
    with pytest.raises(IngressValidationError) as caught:
        _normalize_sync(change, cursor=cursor)

    assert caught.value.safe_code is code


@pytest.mark.parametrize(
    ("account_id", "folder", "cursor", "code"),
    [
        (True, "INBOX", "cursor-1", IngressValidationCode.ACCOUNT_INVALID),
        (0, "INBOX", "cursor-1", IngressValidationCode.ACCOUNT_INVALID),
        (2**63, "INBOX", "cursor-1", IngressValidationCode.ACCOUNT_INVALID),
        (8, " ", "cursor-1", IngressValidationCode.FOLDER_INVALID),
        (8, "INBOX\x01", "cursor-1", IngressValidationCode.FOLDER_INVALID),
        (8, "\ud800", "cursor-1", IngressValidationCode.FOLDER_INVALID),
        (8, "INBOX", " ", IngressValidationCode.CURSOR_INVALID),
        (8, "INBOX", "\ud800", IngressValidationCode.CURSOR_INVALID),
    ],
)
def test_sync_rejects_invalid_boundary_fields(
    account_id: int,
    folder: str,
    cursor: str,
    code: IngressValidationCode,
) -> None:
    with pytest.raises(IngressValidationError) as caught:
        _normalize_sync(account_id=account_id, folder=folder, cursor=cursor)

    assert caught.value.safe_code is code


def test_sync_dedupe_includes_cursor_kind_and_locked_identity() -> None:
    created = _normalize_sync()
    next_cursor = _normalize_sync(cursor="cursor-2")
    deleted = _normalize_sync(
        SyncChange(ChangeKind.DELETE, "message-1", None, "version-1")
    )

    assert created.dedupe_key != next_cursor.dedupe_key
    assert created.dedupe_key != deleted.dedupe_key

    identity = {
        "schema_version": 1,
        "account_id": 8,
        "source": "sync",
        "raw_event_type": "create",
        "kind": "create",
        "external_email_id": "message-1",
        "folder": "INBOX",
        "source_version": "version-1",
        "cursor": "cursor-1",
        "source_event_at": None,
        "raw_body_sha256": None,
    }
    canonical = json.dumps(
        identity,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    assert created.dedupe_key == hashlib.sha256(canonical).hexdigest()
    assert (
        created.dedupe_key
        == "316a5c7efda42a02e6573cf24a994a8df7d23c4bd4f09ab44a396911ebe38b9e"
    )


def test_sync_policy_is_excluded_from_dedupe_identity() -> None:
    full = _normalize_sync(processing_policy=ProcessingPolicy.FULL)
    ignored = _normalize_sync(processing_policy=ProcessingPolicy.IGNORED)

    assert ignored.processing_policy is ProcessingPolicy.IGNORED
    assert ignored.dedupe_key == full.dedupe_key


def test_ingress_validation_error_has_fixed_safe_shape() -> None:
    error = IngressValidationError(IngressValidationCode.EVENT_MISSING)

    assert error.kind is ErrorKind.VALIDATION
    assert error.safe_code is IngressValidationCode.EVENT_MISSING
    assert error.safe_summary == "Invalid ingress event"
    assert str(error) == "Invalid ingress event"
    assert repr(error) == ("IngressValidationError(safe_code='ingress.event_missing')")
    assert error.__cause__ is None
    assert not isinstance(error, InputLimitExceeded)


def test_ingress_validation_error_rejects_arbitrary_codes() -> None:
    with pytest.raises((TypeError, ValueError)):
        IngressValidationError("secret-value")  # type: ignore[arg-type]


def test_validation_errors_do_not_leak_raw_identifiers_or_causes(
    caplog: pytest.LogCaptureFixture,
) -> None:
    sentinel = "TOP-SECRET-MESSAGE-ID"
    payload = _webhook_payload(id=sentinel)

    with pytest.raises(IngressValidationError) as caught:
        _normalize_webhook(payload)

    error = caught.value
    logging.getLogger("test.ingress").warning("rejected: %r", error)
    rendered = " ".join([str(error), repr(error), repr(vars(error)), caplog.text])
    assert sentinel not in rendered
    assert error.__cause__ is None
