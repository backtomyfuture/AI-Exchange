"""Normalize trusted intake data into stable durable-ingestion events."""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Final, TypeAlias

from src.domain.errors import IngressValidationCode, IngressValidationError
from src.ingestion.folder_identity import canonicalize_folder_identity
from src.ingestion.models import (
    ChangeKind,
    IngressSource,
    NormalizedIngressEvent,
    POSTGRES_BIGINT_MAX,
    ProcessingPolicy,
    SyncChange,
)


_DEDUPE_SCHEMA_VERSION: Final = 1
_EVENT_KINDS: Final = {
    "NewMailEvent": ChangeKind.CREATE,
    "CreatedEvent": ChangeKind.CREATE,
    "ModifiedEvent": ChangeKind.UPDATE,
    "DeletedEvent": ChangeKind.DELETE,
}
_TEST_EVENT_TYPE: Final = "TestEvent"
_TEST_EVENT_KEYS: Final = frozenset({"event", "timestamp", "account_id", "message"})
_SUPPORTED_SYNC_KINDS: Final = frozenset(
    {ChangeKind.CREATE, ChangeKind.UPDATE, ChangeKind.DELETE}
)
_SYNC_TRANSPORT_KEYS: Final = frozenset({"change_type", "id", "item"})
_SYNC_ITEM_ALLOWED_KEYS: Final = frozenset(
    {
        "id",
        "subject",
        "sender",
        "received_time",
        "is_read",
        "has_attachments",
    }
)
_SYNC_RECEIVED_TIME_PATTERN: Final = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
)
_SOURCE_EVENT_AT_MIN: Final = datetime(2, 1, 1, tzinfo=UTC)
_SOURCE_EVENT_AT_MAX_EXCLUSIVE: Final = datetime(9999, 1, 1, tzinfo=UTC)


class _InvalidJson(ValueError):
    pass


def _raise(code: IngressValidationCode) -> None:
    raise IngressValidationError(code)


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise _InvalidJson
        result[key] = value
    return result


def _reject_json_constant(_value: str) -> None:
    raise _InvalidJson


def _parse_signed_body(raw_body: object) -> Mapping[str, Any]:
    if not isinstance(raw_body, bytes):
        _raise(IngressValidationCode.INVALID_BODY)
    try:
        decoded = raw_body.decode("utf-8")
        parsed = json.loads(
            decoded,
            object_pairs_hook=_unique_object,
            parse_constant=_reject_json_constant,
        )
    except (
        UnicodeDecodeError,
        json.JSONDecodeError,
        _InvalidJson,
        RecursionError,
        ValueError,
    ):
        raise IngressValidationError(IngressValidationCode.INVALID_BODY) from None
    if not isinstance(parsed, Mapping):
        _raise(IngressValidationCode.INVALID_BODY)
    return parsed


def _plain_json(value: object, *, active: set[int]) -> object:
    if value is None or isinstance(value, (bool, int)):
        return value
    if isinstance(value, str):
        if "\x00" in value:
            raise _InvalidJson
        try:
            value.encode("utf-8")
        except UnicodeEncodeError:
            raise _InvalidJson from None
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise _InvalidJson
        return value
    if isinstance(value, Mapping):
        identity = id(value)
        if identity in active:
            raise _InvalidJson
        active.add(identity)
        try:
            result: dict[str, object] = {}
            for key, item in value.items():
                if not isinstance(key, str) or "\x00" in key or key in result:
                    raise _InvalidJson
                try:
                    key.encode("utf-8")
                except UnicodeEncodeError:
                    raise _InvalidJson from None
                result[key] = _plain_json(item, active=active)
            return result
        finally:
            active.remove(identity)
    if isinstance(value, Sequence) and not isinstance(
        value,
        (str, bytes, bytearray),
    ):
        identity = id(value)
        if identity in active:
            raise _InvalidJson
        active.add(identity)
        try:
            return [_plain_json(item, active=active) for item in value]
        finally:
            active.remove(identity)
    raise _InvalidJson


def _canonical_json_bytes(
    value: object,
    *,
    error_code: IngressValidationCode,
) -> bytes:
    try:
        plain = _plain_json(value, active=set())
        return json.dumps(
            plain,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except Exception:
        raise IngressValidationError(error_code) from None


def _require_policy(value: object) -> ProcessingPolicy:
    if not isinstance(value, ProcessingPolicy):
        _raise(IngressValidationCode.POLICY_INVALID)
    return value


def _require_account_id(value: object) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value <= 0
        or value > POSTGRES_BIGINT_MAX
    ):
        _raise(IngressValidationCode.ACCOUNT_INVALID)
    return value


def _require_text(
    value: object,
    *,
    code: IngressValidationCode,
    max_length: int,
) -> str:
    if not isinstance(value, str):
        _raise(code)
    normalized = value.strip()
    if (
        not normalized
        or len(normalized) > max_length
        or any(character in normalized for character in ("\x00", "\r", "\n"))
    ):
        _raise(code)
    try:
        normalized.encode("utf-8")
    except UnicodeEncodeError:
        raise IngressValidationError(code) from None
    return normalized


def _optional_text(
    value: object,
    *,
    code: IngressValidationCode,
    max_length: int,
) -> str | None:
    if value is None:
        return None
    return _require_exact_text(value, code=code, max_length=max_length)


def _normalize_folder(value: object) -> str:
    folder = _require_text(
        value,
        code=IngressValidationCode.FOLDER_INVALID,
        max_length=512,
    )
    try:
        return canonicalize_folder_identity(folder)
    except ValueError:
        raise IngressValidationError(IngressValidationCode.FOLDER_INVALID) from None


def _require_exact_text(
    value: object,
    *,
    code: IngressValidationCode,
    max_length: int,
) -> str:
    if not isinstance(value, str):
        _raise(code)
    if (
        not value
        or value != value.strip()
        or len(value) > max_length
        or any(
            ord(character) <= 0x1F or 0x7F <= ord(character) <= 0x9F
            for character in value
        )
    ):
        _raise(code)
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        raise IngressValidationError(code) from None
    return value


def _validate_sync_item_text(
    item: Mapping[str, object],
    key: str,
    *,
    max_length: int,
) -> None:
    value = item.get(key)
    if value is None:
        return
    if not isinstance(value, str) or len(value) > max_length or "\x00" in value:
        _raise(IngressValidationCode.SYNC_ITEM_INVALID)
    try:
        value.encode("utf-8")
    except Exception:
        raise IngressValidationError(IngressValidationCode.SYNC_ITEM_INVALID) from None


def _mapping_value(
    parent: Mapping[str, Any],
    key: str,
    *,
    code: IngressValidationCode,
    required: bool,
) -> Mapping[str, Any] | None:
    if key not in parent or parent[key] is None:
        if required:
            _raise(code)
        return None
    value = parent[key]
    if not isinstance(value, Mapping):
        _raise(code)
    return value


def _nested_text(
    parent: Mapping[str, Any],
    key: str,
    nested_key: str,
    *,
    code: IngressValidationCode,
    max_length: int,
    required_container: bool = False,
    required_value: bool = False,
    trim: bool = False,
) -> str | None:
    container = _mapping_value(
        parent,
        key,
        code=code,
        required=required_container,
    )
    if container is None:
        return None
    if nested_key not in container or container[nested_key] is None:
        if required_value:
            _raise(code)
        return None
    validator = _require_text if trim else _require_exact_text
    return validator(container[nested_key], code=code, max_length=max_length)


def _consistent_value(
    candidates: Sequence[str | None],
    *,
    missing_code: IngressValidationCode,
    conflict_code: IngressValidationCode,
) -> str:
    present = [value for value in candidates if value is not None]
    if not present:
        _raise(missing_code)
    if any(value != present[0] for value in present[1:]):
        _raise(conflict_code)
    return present[0]


def _signed_event_type(payload: Mapping[str, Any]) -> str:
    has_event = "event" in payload
    has_event_type = "event_type" in payload
    if not has_event and not has_event_type:
        _raise(IngressValidationCode.EVENT_MISSING)
    if has_event and has_event_type:
        if payload["event"] is None and payload["event_type"] is None:
            _raise(IngressValidationCode.EVENT_MISSING)
        if payload["event"] is None or payload["event_type"] is None:
            _raise(IngressValidationCode.EVENT_CONFLICT)
        event = _require_exact_text(
            payload["event"],
            code=IngressValidationCode.EVENT_UNSUPPORTED,
            max_length=128,
        )
        event_type = _require_exact_text(
            payload["event_type"],
            code=IngressValidationCode.EVENT_UNSUPPORTED,
            max_length=128,
        )
        if event != event_type:
            _raise(IngressValidationCode.EVENT_CONFLICT)
        raw_event_type = event_type
    else:
        value = payload["event"] if has_event else payload["event_type"]
        if value is None:
            _raise(IngressValidationCode.EVENT_MISSING)
        raw_event_type = _require_exact_text(
            value,
            code=IngressValidationCode.EVENT_UNSUPPORTED,
            max_length=128,
        )
    return raw_event_type


def _email_id(payload: Mapping[str, Any]) -> str:
    item_id = _nested_text(
        payload,
        "item_id",
        "id",
        code=IngressValidationCode.EMAIL_ID_INVALID,
        max_length=1024,
        required_container="item_id" in payload,
        required_value="item_id" in payload,
    )
    top_id = (
        _require_exact_text(
            payload["id"],
            code=IngressValidationCode.EMAIL_ID_INVALID,
            max_length=1024,
        )
        if "id" in payload and payload["id"] is not None
        else None
    )
    item = _mapping_value(
        payload,
        "item",
        code=IngressValidationCode.EMAIL_ID_INVALID,
        required=False,
    )
    nested_id = (
        _require_exact_text(
            item["id"],
            code=IngressValidationCode.EMAIL_ID_INVALID,
            max_length=1024,
        )
        if item is not None and "id" in item and item["id"] is not None
        else None
    )
    return _consistent_value(
        (item_id, top_id, nested_id),
        missing_code=IngressValidationCode.EMAIL_ID_INVALID,
        conflict_code=IngressValidationCode.EMAIL_ID_CONFLICT,
    )


def _folder(payload: Mapping[str, Any]) -> str:
    authoritative = _nested_text(
        payload,
        "parent_folder_id",
        "id",
        code=IngressValidationCode.FOLDER_INVALID,
        max_length=512,
        required_container=True,
        required_value=True,
        trim=True,
    )
    assert authoritative is not None
    candidates: list[str] = [_normalize_folder(authoritative)]

    folder_id = _nested_text(
        payload,
        "folder_id",
        "id",
        code=IngressValidationCode.FOLDER_INVALID,
        max_length=512,
        trim=True,
    )
    if folder_id is not None:
        candidates.append(_normalize_folder(folder_id))
    if "folder" in payload and payload["folder"] is not None:
        candidates.append(_normalize_folder(payload["folder"]))

    item = _mapping_value(
        payload,
        "item",
        code=IngressValidationCode.FOLDER_INVALID,
        required=False,
    )
    if item is not None:
        item_folder = _nested_text(
            item,
            "parent_folder_id",
            "id",
            code=IngressValidationCode.FOLDER_INVALID,
            max_length=512,
            trim=True,
        )
        if item_folder is not None:
            candidates.append(_normalize_folder(item_folder))

    if any(candidate != candidates[0] for candidate in candidates[1:]):
        _raise(IngressValidationCode.FOLDER_CONFLICT)
    return candidates[0]


def _source_version(payload: Mapping[str, Any]) -> str | None:
    item_changekey = _nested_text(
        payload,
        "item_id",
        "changekey",
        code=IngressValidationCode.VERSION_INVALID,
        max_length=512,
    )
    top_changekey = (
        _require_exact_text(
            payload["changekey"],
            code=IngressValidationCode.VERSION_INVALID,
            max_length=512,
        )
        if "changekey" in payload and payload["changekey"] is not None
        else None
    )
    item = _mapping_value(
        payload,
        "item",
        code=IngressValidationCode.VERSION_INVALID,
        required=False,
    )
    nested_changekey = (
        _require_exact_text(
            item["changekey"],
            code=IngressValidationCode.VERSION_INVALID,
            max_length=512,
        )
        if item is not None and "changekey" in item and item["changekey"] is not None
        else None
    )
    changekeys = [
        value
        for value in (item_changekey, top_changekey, nested_changekey)
        if value is not None
    ]
    if changekeys:
        if any(value != changekeys[0] for value in changekeys[1:]):
            _raise(IngressValidationCode.VERSION_CONFLICT)
        if "watermark" in payload and payload["watermark"] is not None:
            _require_exact_text(
                payload["watermark"],
                code=IngressValidationCode.VERSION_INVALID,
                max_length=512,
            )
        return changekeys[0]
    if "watermark" not in payload or payload["watermark"] is None:
        return None
    return _require_exact_text(
        payload["watermark"],
        code=IngressValidationCode.VERSION_INVALID,
        max_length=512,
    )


def _require_source_event_at_range(value: datetime) -> datetime:
    if not (_SOURCE_EVENT_AT_MIN <= value < _SOURCE_EVENT_AT_MAX_EXCLUSIVE):
        _raise(IngressValidationCode.TIMESTAMP_INVALID)
    return value


def _source_event_at(payload: Mapping[str, Any]) -> datetime | None:
    if "timestamp" not in payload or payload["timestamp"] is None:
        return None
    value = payload["timestamp"]
    if isinstance(value, bool):
        _raise(IngressValidationCode.TIMESTAMP_INVALID)
    if isinstance(value, int):
        try:
            parsed = datetime.fromtimestamp(value, UTC)
        except (OverflowError, OSError, ValueError):
            raise IngressValidationError(
                IngressValidationCode.TIMESTAMP_INVALID
            ) from None
        return _require_source_event_at_range(parsed)
    if isinstance(value, float):
        if not math.isfinite(value):
            _raise(IngressValidationCode.TIMESTAMP_INVALID)
        try:
            parsed = datetime.fromtimestamp(value, UTC)
        except (OverflowError, OSError, ValueError):
            raise IngressValidationError(
                IngressValidationCode.TIMESTAMP_INVALID
            ) from None
        return _require_source_event_at_range(parsed)
    if not isinstance(value, str):
        _raise(IngressValidationCode.TIMESTAMP_INVALID)
    candidate = value.strip()
    if candidate.endswith("Z"):
        candidate = f"{candidate[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(candidate)
        offset = parsed.utcoffset()
        if parsed.tzinfo is None or offset is None:
            _raise(IngressValidationCode.TIMESTAMP_INVALID)
        return _require_source_event_at_range(parsed.astimezone(UTC))
    except (TypeError, ValueError, OverflowError):
        raise IngressValidationError(IngressValidationCode.TIMESTAMP_INVALID) from None


@dataclass(frozen=True, slots=True)
class VerifiedMailWebhookEnvelope:
    """Trusted mail identity parsed once from the exact signed request bytes."""

    account_id: int
    raw_event_type: str
    change_kind: ChangeKind
    external_email_id: str
    exact_folder_identity: str
    source_version: str | None
    source_event_at: datetime | None


@dataclass(frozen=True, slots=True)
class VerifiedTestWebhookEnvelope:
    """Trusted, side-effect-free Exchange extension test event."""

    account_id: int


VerifiedWebhookEnvelope: TypeAlias = (
    VerifiedMailWebhookEnvelope | VerifiedTestWebhookEnvelope
)


@dataclass(frozen=True, slots=True)
class _VerifiedWebhookRequest:
    envelope: VerifiedWebhookEnvelope
    signed_payload: Mapping[str, Any]
    raw_body_sha256: str


def verify_webhook_request(
    *,
    raw_body: bytes,
    payload: Mapping[str, Any],
    header_event: str | None,
    expected_account_id: int | None,
) -> _VerifiedWebhookRequest:
    """Parse one signed representation and bind it to the configured account."""

    signed_payload = _parse_signed_body(raw_body)
    if not isinstance(payload, Mapping):
        _raise(IngressValidationCode.BODY_PAYLOAD_MISMATCH)
    signed_canonical = _canonical_json_bytes(
        signed_payload,
        error_code=IngressValidationCode.INVALID_BODY,
    )
    supplied_canonical = _canonical_json_bytes(
        payload,
        error_code=IngressValidationCode.BODY_PAYLOAD_MISMATCH,
    )
    if signed_canonical != supplied_canonical:
        _raise(IngressValidationCode.BODY_PAYLOAD_MISMATCH)

    raw_event_type = _signed_event_type(signed_payload)
    if header_event is not None:
        normalized_header = _require_exact_text(
            header_event,
            code=IngressValidationCode.HEADER_EVENT_MISMATCH,
            max_length=128,
        )
        if normalized_header != raw_event_type:
            _raise(IngressValidationCode.HEADER_EVENT_MISMATCH)

    account_id = _require_account_id(signed_payload.get("account_id"))
    if expected_account_id is not None:
        configured_account_id = _require_account_id(expected_account_id)
        if account_id != configured_account_id:
            _raise(IngressValidationCode.ACCOUNT_INVALID)

    raw_body_sha256 = hashlib.sha256(raw_body).hexdigest()
    if raw_event_type == _TEST_EVENT_TYPE:
        if frozenset(signed_payload) != _TEST_EVENT_KEYS:
            _raise(IngressValidationCode.EVENT_UNSUPPORTED)
        timestamp = signed_payload.get("timestamp")
        if type(timestamp) is not int or timestamp < 0 or timestamp > 253_402_300_799:
            _raise(IngressValidationCode.TIMESTAMP_INVALID)
        _require_exact_text(
            signed_payload.get("message"),
            code=IngressValidationCode.EVENT_UNSUPPORTED,
            max_length=2048,
        )
        return _VerifiedWebhookRequest(
            envelope=VerifiedTestWebhookEnvelope(account_id=account_id),
            signed_payload=signed_payload,
            raw_body_sha256=raw_body_sha256,
        )

    if raw_event_type not in _EVENT_KINDS:
        _raise(IngressValidationCode.EVENT_UNSUPPORTED)
    source_event_at = _source_event_at(signed_payload)
    return _VerifiedWebhookRequest(
        envelope=VerifiedMailWebhookEnvelope(
            account_id=account_id,
            raw_event_type=raw_event_type,
            change_kind=_EVENT_KINDS[raw_event_type],
            external_email_id=_email_id(signed_payload),
            exact_folder_identity=_folder(signed_payload),
            source_version=_source_version(signed_payload),
            source_event_at=source_event_at,
        ),
        signed_payload=signed_payload,
        raw_body_sha256=raw_body_sha256,
    )


def _dedupe_key(
    *,
    account_id: int,
    source: IngressSource,
    raw_event_type: str,
    kind: ChangeKind,
    external_email_id: str,
    folder: str,
    source_version: str | None,
    cursor: str | None,
    source_event_at: datetime | None,
    raw_body_sha256: str | None,
) -> str:
    identity = {
        "schema_version": _DEDUPE_SCHEMA_VERSION,
        "account_id": account_id,
        "source": source.value,
        "raw_event_type": raw_event_type,
        "kind": kind.value,
        "external_email_id": external_email_id,
        "folder": folder,
        "source_version": source_version,
        "cursor": cursor,
        "source_event_at": (
            source_event_at.astimezone(UTC).isoformat()
            if source_event_at is not None
            else None
        ),
        "raw_body_sha256": raw_body_sha256,
    }
    canonical = _canonical_json_bytes(
        identity,
        error_code=IngressValidationCode.CANONICALIZATION_INVALID,
    )
    return hashlib.sha256(canonical).hexdigest()


def _build_event(**values: Any) -> NormalizedIngressEvent:
    try:
        return NormalizedIngressEvent(**values)
    except (TypeError, ValueError, UnicodeError, RecursionError):
        raise IngressValidationError(
            IngressValidationCode.NORMALIZED_EVENT_INVALID
        ) from None


def normalize_verified_webhook_request(
    verified: _VerifiedWebhookRequest,
    *,
    processing_policy: ProcessingPolicy,
) -> NormalizedIngressEvent:
    """Bind one already-verified mail envelope to one immutable policy fact."""

    if type(verified) is not _VerifiedWebhookRequest:
        _raise(IngressValidationCode.NORMALIZED_EVENT_INVALID)
    envelope = verified.envelope
    if type(envelope) is not VerifiedMailWebhookEnvelope:
        _raise(IngressValidationCode.EVENT_UNSUPPORTED)
    policy = _require_policy(processing_policy)

    if envelope.source_version is not None:
        identity_time = None
        identity_raw_hash = None
    elif envelope.source_event_at is not None:
        identity_time = envelope.source_event_at
        identity_raw_hash = None
    else:
        identity_time = None
        identity_raw_hash = verified.raw_body_sha256
    dedupe_key = _dedupe_key(
        account_id=envelope.account_id,
        source=IngressSource.WEBHOOK,
        raw_event_type=envelope.raw_event_type,
        kind=envelope.change_kind,
        external_email_id=envelope.external_email_id,
        folder=envelope.exact_folder_identity,
        source_version=envelope.source_version,
        cursor=None,
        source_event_at=identity_time,
        raw_body_sha256=identity_raw_hash,
    )
    return _build_event(
        account_id=envelope.account_id,
        source=IngressSource.WEBHOOK,
        raw_event_type=envelope.raw_event_type,
        kind=envelope.change_kind,
        external_email_id=envelope.external_email_id,
        folder=envelope.exact_folder_identity,
        source_version=envelope.source_version,
        dedupe_key=dedupe_key,
        payload=verified.signed_payload,
        processing_policy=policy,
        source_event_at=envelope.source_event_at,
    )


def normalize_webhook_event(
    *,
    raw_body: bytes,
    payload: Mapping[str, Any],
    processing_policy: ProcessingPolicy,
    header_event: str | None = None,
) -> NormalizedIngressEvent:
    """Normalize a verified Webhook body without trusting unsigned headers."""

    verified = verify_webhook_request(
        raw_body=raw_body,
        payload=payload,
        header_event=header_event,
        expected_account_id=None,
    )
    return normalize_verified_webhook_request(
        verified,
        processing_policy=processing_policy,
    )


def validate_sync_change_contract(
    change: Mapping[str, Any] | SyncChange,
) -> SyncChange:
    """Return a clean Sync DTO after validating the transport contract."""

    is_transport_mapping = False
    if isinstance(change, SyncChange):
        try:
            kind = change.kind
            raw_external_email_id = change.external_email_id
            item = change.item
            raw_source_version = change.source_version
        except Exception:
            raise IngressValidationError(
                IngressValidationCode.SYNC_CHANGE_INVALID
            ) from None
    elif isinstance(change, Mapping):
        is_transport_mapping = True
        try:
            transport = _plain_json(change, active=set())
            if not isinstance(transport, dict):
                _raise(IngressValidationCode.SYNC_CHANGE_INVALID)
            transport_keys = frozenset(transport)
            if transport_keys != _SYNC_TRANSPORT_KEYS:
                _raise(IngressValidationCode.SYNC_CHANGE_INVALID)
            raw_kind = transport["change_type"]
            if not isinstance(raw_kind, str):
                _raise(IngressValidationCode.SYNC_CHANGE_INVALID)
            kind = ChangeKind(raw_kind)
            raw_external_email_id = transport["id"]
            item = transport["item"]
            raw_source_version = None
        except IngressValidationError:
            raise
        except Exception:
            raise IngressValidationError(
                IngressValidationCode.SYNC_CHANGE_INVALID
            ) from None
    else:
        _raise(IngressValidationCode.SYNC_CHANGE_INVALID)
    if not isinstance(kind, ChangeKind) or kind not in _SUPPORTED_SYNC_KINDS:
        _raise(IngressValidationCode.SYNC_CHANGE_INVALID)

    try:
        external_email_id = _require_exact_text(
            raw_external_email_id,
            code=IngressValidationCode.EMAIL_ID_INVALID,
            max_length=1024,
        )
    except IngressValidationError:
        raise
    except Exception:
        raise IngressValidationError(IngressValidationCode.EMAIL_ID_INVALID) from None
    try:
        source_version = _optional_text(
            raw_source_version,
            code=IngressValidationCode.VERSION_INVALID,
            max_length=512,
        )
    except IngressValidationError:
        raise
    except Exception:
        raise IngressValidationError(IngressValidationCode.VERSION_INVALID) from None

    if item is not None and not isinstance(item, Mapping):
        _raise(IngressValidationCode.SYNC_ITEM_INVALID)
    if kind in (ChangeKind.CREATE, ChangeKind.UPDATE):
        if item is None:
            _raise(IngressValidationCode.SYNC_ITEM_INVALID)
    elif item is not None:
        _raise(IngressValidationCode.SYNC_ITEM_INVALID)

    try:
        normalized_item = _plain_json(item, active=set()) if item is not None else None
        if normalized_item is not None and not isinstance(normalized_item, dict):
            _raise(IngressValidationCode.SYNC_ITEM_INVALID)
        if normalized_item is not None:
            item_keys = frozenset(normalized_item)
            if (is_transport_mapping and item_keys != _SYNC_ITEM_ALLOWED_KEYS) or (
                not is_transport_mapping
                and not item_keys.issubset(_SYNC_ITEM_ALLOWED_KEYS)
            ):
                _raise(IngressValidationCode.SYNC_ITEM_INVALID)
            if "id" in normalized_item:
                item_id = _require_exact_text(
                    normalized_item["id"],
                    code=IngressValidationCode.SYNC_ITEM_INVALID,
                    max_length=1024,
                )
                if item_id != external_email_id:
                    _raise(IngressValidationCode.SYNC_ITEM_ID_CONFLICT)
            _validate_sync_item_text(
                normalized_item,
                "subject",
                max_length=32_768,
            )
            _validate_sync_item_text(
                normalized_item,
                "sender",
                max_length=2_048,
            )
            _validate_sync_item_text(
                normalized_item,
                "received_time",
                max_length=128,
            )
            received_time = normalized_item.get("received_time")
            if received_time is not None:
                try:
                    if _SYNC_RECEIVED_TIME_PATTERN.fullmatch(received_time) is None:
                        _raise(IngressValidationCode.SYNC_ITEM_INVALID)
                    datetime.fromisoformat(received_time)
                except (TypeError, ValueError):
                    raise IngressValidationError(
                        IngressValidationCode.SYNC_ITEM_INVALID
                    ) from None
            for boolean_key in ("is_read", "has_attachments"):
                if boolean_key in normalized_item and not isinstance(
                    normalized_item[boolean_key], bool
                ):
                    _raise(IngressValidationCode.SYNC_ITEM_INVALID)
    except IngressValidationError:
        raise
    except Exception:
        raise IngressValidationError(IngressValidationCode.SYNC_ITEM_INVALID) from None

    try:
        return SyncChange(
            kind=kind,
            external_email_id=external_email_id,
            item=normalized_item,
            source_version=source_version,
        )
    except Exception:
        raise IngressValidationError(IngressValidationCode.SYNC_ITEM_INVALID) from None


def normalize_sync_change(
    account_id: int,
    folder: str,
    cursor: str,
    change: SyncChange,
    *,
    processing_policy: ProcessingPolicy,
) -> NormalizedIngressEvent:
    """Normalize a single typed Exchange sync change."""

    normalized_account_id = _require_account_id(account_id)
    normalized_folder = _normalize_folder(folder)
    normalized_cursor = _require_exact_text(
        cursor,
        code=IngressValidationCode.CURSOR_INVALID,
        max_length=8192,
    )
    policy = _require_policy(processing_policy)
    validated_change = validate_sync_change_contract(change)
    kind = validated_change.kind
    external_email_id = validated_change.external_email_id
    item = validated_change.item
    source_version = validated_change.source_version
    try:
        normalized_item = _plain_json(item, active=set()) if item is not None else None
    except Exception:
        raise IngressValidationError(IngressValidationCode.SYNC_ITEM_INVALID) from None
    payload = {
        "cursor": normalized_cursor,
        "change_type": kind.value,
        "id": external_email_id,
        "item": normalized_item,
        "source_version": source_version,
    }
    dedupe_key = _dedupe_key(
        account_id=normalized_account_id,
        source=IngressSource.SYNC,
        raw_event_type=kind.value,
        kind=kind,
        external_email_id=external_email_id,
        folder=normalized_folder,
        source_version=source_version,
        cursor=normalized_cursor,
        source_event_at=None,
        raw_body_sha256=None,
    )
    return _build_event(
        account_id=normalized_account_id,
        source=IngressSource.SYNC,
        raw_event_type=kind.value,
        kind=kind,
        external_email_id=external_email_id,
        folder=normalized_folder,
        source_version=source_version,
        dedupe_key=dedupe_key,
        payload=payload,
        processing_policy=policy,
        source_event_at=None,
    )


__all__ = [
    "normalize_sync_change",
    "normalize_webhook_event",
    "validate_sync_change_contract",
]
