"""Verify cryptographically bound receipts for checkpoint-cleanup backups.

This module only validates proof that an external full-database backup completed.
It does not create backups and it does not authorize or perform deletion by itself.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Protocol


BACKUP_RECEIPT_VERSION = 1
BACKUP_RECEIPT_SCOPE = "full_database"
BACKUP_RECEIPT_STATUS = "completed"
MAX_BACKUP_RECEIPT_BYTES = 16 * 1024
MIN_HMAC_KEY_BYTES = 32

_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_REQUIRED_FIELDS = frozenset(
    {
        "version",
        "plan_id",
        "database_fingerprint",
        "alembic_revision",
        "checkpoint_revision",
        "backup_id",
        "completed_at",
        "scope",
        "manifest_sha256",
        "status",
        "signature",
    }
)
_SAFE_ERROR_CODES = frozenset(
    {
        "backup_receipt_invalid",
        "backup_receipt_key_invalid",
        "backup_receipt_too_large",
        "backup_receipt_malformed",
        "backup_receipt_duplicate_key",
        "backup_receipt_schema_invalid",
        "backup_receipt_not_canonical",
        "backup_receipt_version_invalid",
        "backup_receipt_scope_invalid",
        "backup_receipt_status_invalid",
        "backup_receipt_plan_invalid",
        "backup_receipt_database_invalid",
        "backup_receipt_plan_mismatch",
        "backup_receipt_database_mismatch",
        "backup_receipt_alembic_revision_invalid",
        "backup_receipt_alembic_revision_mismatch",
        "backup_receipt_checkpoint_revision_invalid",
        "backup_receipt_checkpoint_revision_mismatch",
        "backup_receipt_backup_id_invalid",
        "backup_receipt_manifest_invalid",
        "backup_receipt_completed_at_invalid",
        "backup_receipt_plan_created_at_invalid",
        "backup_receipt_completed_before_plan",
        "backup_receipt_signature_invalid",
    }
)


class BackupReceiptError(ValueError):
    """A validation failure whose text is always a fixed, non-sensitive code."""

    def __init__(self, code: str) -> None:
        safe_code = code if code in _SAFE_ERROR_CODES else "backup_receipt_invalid"
        self.code = safe_code
        super().__init__(safe_code)


@dataclass(frozen=True, slots=True)
class VerifiedBackupReceipt:
    """The authenticated claims required by the cleanup execution boundary."""

    version: int
    plan_id: str
    database_fingerprint: str
    alembic_revision: str
    checkpoint_revision: int
    backup_id: str
    completed_at: datetime
    scope: str
    manifest_sha256: str
    status: str


class BackupReceiptVerifier(Protocol):
    """Structural contract used by cleanup orchestration code."""

    def verify(
        self,
        receipt: str | bytes,
        *,
        expected_plan_id: str,
        expected_database_fingerprint: str,
        expected_alembic_revision: str,
        expected_checkpoint_revision: int,
        plan_created_at: datetime,
    ) -> VerifiedBackupReceipt: ...


class _DuplicateJsonKey(ValueError):
    pass


def _canonical_json(payload: dict[str, object]) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _reject_json_constant(_: str) -> None:
    raise ValueError


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicateJsonKey
        result[key] = value
    return result


def _validate_key(key: bytes) -> bytes:
    if type(key) is not bytes or len(key) < MIN_HMAC_KEY_BYTES:
        raise BackupReceiptError("backup_receipt_key_invalid")
    return key


def _is_nonempty_string(value: object, *, max_length: int = 512) -> bool:
    return (
        isinstance(value, str)
        and bool(value.strip())
        and value == value.strip()
        and len(value) <= max_length
        and not any(ord(character) < 32 or ord(character) == 127 for character in value)
    )


def _validate_identifier(value: object, code: str) -> str:
    if not _is_nonempty_string(value):
        raise BackupReceiptError(code)
    return value


def _validate_digest(value: object, code: str) -> str:
    if not isinstance(value, str) or _SHA256_PATTERN.fullmatch(value) is None:
        raise BackupReceiptError(code)
    return value


def _validate_checkpoint_revision(value: object) -> int:
    if type(value) is not int or not 1 <= value <= 100_000:
        raise BackupReceiptError("backup_receipt_checkpoint_revision_invalid")
    return value


def _require_aware_utc(value: object, code: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise BackupReceiptError(code)
    try:
        offset = value.utcoffset()
    except Exception:
        raise BackupReceiptError(code) from None
    if offset != timedelta(0):
        raise BackupReceiptError(code)
    return value.astimezone(timezone.utc)


def _parse_utc_timestamp(value: object) -> datetime:
    if not isinstance(value, str) or not value:
        raise BackupReceiptError("backup_receipt_completed_at_invalid")
    normalized = f"{value[:-1]}+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(normalized)
    except (TypeError, ValueError, OverflowError):
        raise BackupReceiptError("backup_receipt_completed_at_invalid") from None
    return _require_aware_utc(parsed, "backup_receipt_completed_at_invalid")


def _format_utc_timestamp(value: object) -> str:
    parsed = _require_aware_utc(value, "backup_receipt_completed_at_invalid")
    return parsed.isoformat().replace("+00:00", "Z")


def _parse_receipt(receipt: str | bytes) -> tuple[dict[str, object], bytes]:
    if isinstance(receipt, str):
        try:
            raw = receipt.encode("utf-8")
        except UnicodeEncodeError:
            raise BackupReceiptError("backup_receipt_malformed") from None
    elif type(receipt) is bytes:
        raw = receipt
    else:
        raise BackupReceiptError("backup_receipt_malformed")

    if not raw:
        raise BackupReceiptError("backup_receipt_malformed")
    if len(raw) > MAX_BACKUP_RECEIPT_BYTES:
        raise BackupReceiptError("backup_receipt_too_large")

    try:
        text = raw.decode("utf-8")
        parsed = json.loads(
            text,
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=_reject_json_constant,
        )
    except _DuplicateJsonKey:
        raise BackupReceiptError("backup_receipt_duplicate_key") from None
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError, RecursionError):
        raise BackupReceiptError("backup_receipt_malformed") from None

    if not isinstance(parsed, dict):
        raise BackupReceiptError("backup_receipt_schema_invalid")
    return parsed, raw


def _validate_claims(
    payload: dict[str, object],
) -> tuple[str, str, str, int, str, datetime, str, str]:
    if frozenset(payload) != _REQUIRED_FIELDS:
        raise BackupReceiptError("backup_receipt_schema_invalid")

    if type(payload["version"]) is not int or payload["version"] != BACKUP_RECEIPT_VERSION:
        raise BackupReceiptError("backup_receipt_version_invalid")
    if payload["scope"] != BACKUP_RECEIPT_SCOPE:
        raise BackupReceiptError("backup_receipt_scope_invalid")
    if payload["status"] != BACKUP_RECEIPT_STATUS:
        raise BackupReceiptError("backup_receipt_status_invalid")

    plan_id = _validate_digest(payload["plan_id"], "backup_receipt_plan_invalid")
    database_fingerprint = _validate_digest(
        payload["database_fingerprint"],
        "backup_receipt_database_invalid",
    )
    alembic_revision = _validate_identifier(
        payload["alembic_revision"],
        "backup_receipt_alembic_revision_invalid",
    )
    checkpoint_revision = _validate_checkpoint_revision(
        payload["checkpoint_revision"]
    )
    backup_id = _validate_identifier(
        payload["backup_id"],
        "backup_receipt_backup_id_invalid",
    )

    manifest_sha256 = payload["manifest_sha256"]
    if not isinstance(manifest_sha256, str) or not _SHA256_PATTERN.fullmatch(
        manifest_sha256
    ):
        raise BackupReceiptError("backup_receipt_manifest_invalid")

    signature = payload["signature"]
    if not isinstance(signature, str) or not _SHA256_PATTERN.fullmatch(signature):
        raise BackupReceiptError("backup_receipt_signature_invalid")

    completed_at = _parse_utc_timestamp(payload["completed_at"])
    return (
        plan_id,
        database_fingerprint,
        alembic_revision,
        checkpoint_revision,
        backup_id,
        completed_at,
        manifest_sha256,
        signature,
    )


class HmacBackupReceiptVerifier:
    """Authenticate canonical backup receipts with HMAC-SHA256."""

    __slots__ = ("_key",)

    def __init__(self, key: bytes) -> None:
        self._key = _validate_key(key)

    def verify(
        self,
        receipt: str | bytes,
        *,
        expected_plan_id: str,
        expected_database_fingerprint: str,
        expected_alembic_revision: str,
        expected_checkpoint_revision: int,
        plan_created_at: datetime,
    ) -> VerifiedBackupReceipt:
        expected_plan = _validate_digest(
            expected_plan_id,
            "backup_receipt_plan_invalid",
        )
        expected_database = _validate_digest(
            expected_database_fingerprint,
            "backup_receipt_database_invalid",
        )
        expected_alembic = _validate_identifier(
            expected_alembic_revision,
            "backup_receipt_alembic_revision_invalid",
        )
        expected_checkpoint = _validate_checkpoint_revision(
            expected_checkpoint_revision
        )
        created_at = _require_aware_utc(
            plan_created_at,
            "backup_receipt_plan_created_at_invalid",
        )

        payload, raw = _parse_receipt(receipt)
        (
            plan_id,
            database_fingerprint,
            alembic_revision,
            checkpoint_revision,
            backup_id,
            completed_at,
            manifest_sha256,
            signature,
        ) = _validate_claims(payload)

        if raw != _canonical_json(payload):
            raise BackupReceiptError("backup_receipt_not_canonical")

        unsigned_payload = dict(payload)
        del unsigned_payload["signature"]
        expected_signature = hmac.new(
            self._key,
            _canonical_json(unsigned_payload),
            hashlib.sha256,
        ).hexdigest()
        if not hmac.compare_digest(signature, expected_signature):
            raise BackupReceiptError("backup_receipt_signature_invalid")

        if plan_id != expected_plan:
            raise BackupReceiptError("backup_receipt_plan_mismatch")
        if database_fingerprint != expected_database:
            raise BackupReceiptError("backup_receipt_database_mismatch")
        if alembic_revision != expected_alembic:
            raise BackupReceiptError(
                "backup_receipt_alembic_revision_mismatch"
            )
        if checkpoint_revision != expected_checkpoint:
            raise BackupReceiptError(
                "backup_receipt_checkpoint_revision_mismatch"
            )
        if completed_at < created_at:
            raise BackupReceiptError("backup_receipt_completed_before_plan")

        return VerifiedBackupReceipt(
            version=BACKUP_RECEIPT_VERSION,
            plan_id=plan_id,
            database_fingerprint=database_fingerprint,
            alembic_revision=alembic_revision,
            checkpoint_revision=checkpoint_revision,
            backup_id=backup_id,
            completed_at=completed_at,
            scope=BACKUP_RECEIPT_SCOPE,
            manifest_sha256=manifest_sha256,
            status=BACKUP_RECEIPT_STATUS,
        )


def create_signed_backup_receipt(
    *,
    key: bytes,
    plan_id: str,
    database_fingerprint: str,
    alembic_revision: str,
    checkpoint_revision: int,
    backup_id: str,
    completed_at: datetime,
    manifest_sha256: str,
) -> str:
    """Create a canonical receipt for an already completed external backup.

    This pure helper signs caller-supplied backup facts. It performs no I/O and
    deliberately has no capability to create or inspect a database backup.
    """

    signing_key = _validate_key(key)
    validated_plan_id = _validate_digest(plan_id, "backup_receipt_plan_invalid")
    validated_database = _validate_digest(
        database_fingerprint,
        "backup_receipt_database_invalid",
    )
    validated_alembic = _validate_identifier(
        alembic_revision,
        "backup_receipt_alembic_revision_invalid",
    )
    validated_checkpoint = _validate_checkpoint_revision(
        checkpoint_revision
    )
    validated_backup_id = _validate_identifier(
        backup_id,
        "backup_receipt_backup_id_invalid",
    )
    if not isinstance(manifest_sha256, str) or not _SHA256_PATTERN.fullmatch(
        manifest_sha256
    ):
        raise BackupReceiptError("backup_receipt_manifest_invalid")

    payload: dict[str, object] = {
        "version": BACKUP_RECEIPT_VERSION,
        "plan_id": validated_plan_id,
        "database_fingerprint": validated_database,
        "alembic_revision": validated_alembic,
        "checkpoint_revision": validated_checkpoint,
        "backup_id": validated_backup_id,
        "completed_at": _format_utc_timestamp(completed_at),
        "scope": BACKUP_RECEIPT_SCOPE,
        "manifest_sha256": manifest_sha256,
        "status": BACKUP_RECEIPT_STATUS,
    }
    payload["signature"] = hmac.new(
        signing_key,
        _canonical_json(payload),
        hashlib.sha256,
    ).hexdigest()
    return _canonical_json(payload).decode("utf-8")
