"""Validate Exchange ``sync_state`` deltas before durable persistence."""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any, Final

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


_DEDUPE_SCHEMA_VERSION: Final = 2
_SUPPORTED_SYNC_KINDS: Final = frozenset(
    {ChangeKind.CREATE, ChangeKind.UPDATE, ChangeKind.DELETE}
)
_SYNC_TRANSPORT_KEYS: Final = frozenset({"change_type", "id", "item"})
_SYNC_ITEM_ALLOWED_KEYS: Final = frozenset(
    {"id", "subject", "sender", "received_time", "is_read", "has_attachments"}
)
_SYNC_RECEIVED_TIME_PATTERN: Final = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
)


class _InvalidJson(ValueError):
    pass


def _raise(code: IngressValidationCode) -> None:
    raise IngressValidationError(code)


def _plain_json(value: object, *, active: set[int]) -> object:
    """Copy a JSON-compatible value while rejecting cycles and invalid scalars."""

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


def _canonical_json_bytes(value: object) -> bytes:
    try:
        return json.dumps(
            _plain_json(value, active=set()),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except Exception:
        raise IngressValidationError(
            IngressValidationCode.CANONICALIZATION_INVALID
        ) from None


def _require_policy(value: object) -> ProcessingPolicy:
    if not isinstance(value, ProcessingPolicy):
        _raise(IngressValidationCode.POLICY_INVALID)
    return value


def _require_account_id(value: object) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not 1 <= value <= POSTGRES_BIGINT_MAX
    ):
        _raise(IngressValidationCode.ACCOUNT_INVALID)
    return value


def _require_text(
    value: object,
    *,
    code: IngressValidationCode,
    max_length: int,
    trim: bool = False,
) -> str:
    if not isinstance(value, str):
        _raise(code)
    candidate = value.strip() if trim else value
    if (
        not candidate
        or (not trim and candidate != candidate.strip())
        or len(candidate) > max_length
        or any(ord(character) <= 0x1F or 0x7F <= ord(character) <= 0x9F for character in candidate)
    ):
        _raise(code)
    try:
        candidate.encode("utf-8")
    except UnicodeEncodeError:
        raise IngressValidationError(code) from None
    return candidate


def _optional_text(
    value: object,
    *,
    code: IngressValidationCode,
    max_length: int,
) -> str | None:
    if value is None:
        return None
    return _require_text(value, code=code, max_length=max_length)


def _normalize_folder(value: object) -> str:
    folder = _require_text(
        value,
        code=IngressValidationCode.FOLDER_INVALID,
        max_length=512,
        trim=True,
    )
    try:
        return canonicalize_folder_identity(folder)
    except ValueError:
        raise IngressValidationError(IngressValidationCode.FOLDER_INVALID) from None


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
    except UnicodeEncodeError:
        raise IngressValidationError(IngressValidationCode.SYNC_ITEM_INVALID) from None


def validate_sync_change_contract(change: Mapping[str, Any] | SyncChange) -> SyncChange:
    """Return a clean sync DTO after validating the Gateway transport shape."""

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
            if not isinstance(transport, dict) or frozenset(transport) != _SYNC_TRANSPORT_KEYS:
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
        external_email_id = _require_text(
            raw_external_email_id,
            code=IngressValidationCode.EMAIL_ID_INVALID,
            max_length=1024,
        )
        source_version = _optional_text(
            raw_source_version,
            code=IngressValidationCode.VERSION_INVALID,
            max_length=512,
        )
    except IngressValidationError:
        raise
    except Exception:
        raise IngressValidationError(IngressValidationCode.SYNC_CHANGE_INVALID) from None

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
                item_id = _require_text(
                    normalized_item["id"],
                    code=IngressValidationCode.SYNC_ITEM_INVALID,
                    max_length=1024,
                )
                if item_id != external_email_id:
                    _raise(IngressValidationCode.SYNC_ITEM_ID_CONFLICT)
            _validate_sync_item_text(normalized_item, "subject", max_length=32_768)
            _validate_sync_item_text(normalized_item, "sender", max_length=2_048)
            _validate_sync_item_text(normalized_item, "received_time", max_length=128)
            received_time = normalized_item.get("received_time")
            if received_time is not None:
                if (
                    not isinstance(received_time, str)
                    or _SYNC_RECEIVED_TIME_PATTERN.fullmatch(received_time) is None
                ):
                    _raise(IngressValidationCode.SYNC_ITEM_INVALID)
                datetime.fromisoformat(received_time)
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


def _dedupe_key(
    *,
    account_id: int,
    raw_event_type: str,
    kind: ChangeKind,
    external_email_id: str,
    folder: str,
    source_version: str | None,
    cursor: str,
) -> str:
    return hashlib.sha256(
        _canonical_json_bytes(
            {
                "schema_version": _DEDUPE_SCHEMA_VERSION,
                "account_id": account_id,
                "source": IngressSource.SYNC.value,
                "raw_event_type": raw_event_type,
                "kind": kind.value,
                "external_email_id": external_email_id,
                "folder": folder,
                "source_version": source_version,
                "cursor": cursor,
            }
        )
    ).hexdigest()


def normalize_sync_change(
    account_id: int,
    folder: str,
    cursor: str,
    change: SyncChange,
    *,
    processing_policy: ProcessingPolicy,
) -> NormalizedIngressEvent:
    """Normalize one typed Exchange sync change for the durable Inbox."""

    normalized_account_id = _require_account_id(account_id)
    normalized_folder = _normalize_folder(folder)
    normalized_cursor = _require_text(
        cursor,
        code=IngressValidationCode.CURSOR_INVALID,
        max_length=8192,
    )
    policy = _require_policy(processing_policy)
    validated_change = validate_sync_change_contract(change)
    normalized_item = validated_change.item
    payload = {
        "cursor": normalized_cursor,
        "change_type": validated_change.kind.value,
        "id": validated_change.external_email_id,
        "item": normalized_item,
        "source_version": validated_change.source_version,
    }
    try:
        return NormalizedIngressEvent(
            account_id=normalized_account_id,
            source=IngressSource.SYNC,
            raw_event_type=validated_change.kind.value,
            kind=validated_change.kind,
            external_email_id=validated_change.external_email_id,
            folder=normalized_folder,
            source_version=validated_change.source_version,
            dedupe_key=_dedupe_key(
                account_id=normalized_account_id,
                raw_event_type=validated_change.kind.value,
                kind=validated_change.kind,
                external_email_id=validated_change.external_email_id,
                folder=normalized_folder,
                source_version=validated_change.source_version,
                cursor=normalized_cursor,
            ),
            payload=payload,
            processing_policy=policy,
            source_event_at=None,
        )
    except (TypeError, ValueError, UnicodeError, RecursionError):
        raise IngressValidationError(
            IngressValidationCode.NORMALIZED_EVENT_INVALID
        ) from None


__all__ = ["normalize_sync_change", "validate_sync_change_contract"]
