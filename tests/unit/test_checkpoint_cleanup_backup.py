from __future__ import annotations

import hashlib
import hmac
import json
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from src.maintenance.cleanup_backup import (
    MAX_BACKUP_RECEIPT_BYTES,
    BackupReceiptError,
    HmacBackupReceiptVerifier,
    create_signed_backup_receipt,
)


KEY = b"k" * 32
PLAN_ID = "1" * 64
DATABASE_FINGERPRINT = "2" * 64
BACKUP_ID = "backup-20260712-001"
MANIFEST_SHA256 = "a" * 64
ALEMBIC_REVISION = "20260710_0002"
CHECKPOINT_REVISION = 9
PLAN_CREATED_AT = datetime(2026, 7, 12, 1, 0, tzinfo=timezone.utc)
COMPLETED_AT = datetime(2026, 7, 12, 1, 5, tzinfo=timezone.utc)


def _canonical(payload: dict[str, object]) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _signed_receipt(**overrides: object) -> str:
    payload: dict[str, object] = {
        "version": 1,
        "plan_id": PLAN_ID,
        "database_fingerprint": DATABASE_FINGERPRINT,
        "alembic_revision": ALEMBIC_REVISION,
        "checkpoint_revision": CHECKPOINT_REVISION,
        "backup_id": BACKUP_ID,
        "completed_at": "2026-07-12T01:05:00Z",
        "scope": "full_database",
        "manifest_sha256": MANIFEST_SHA256,
        "status": "completed",
    }
    payload.update(overrides)
    signature = hmac.new(KEY, _canonical(payload), hashlib.sha256).hexdigest()
    payload["signature"] = signature
    return _canonical(payload).decode("utf-8")


def _verifier(key: bytes = KEY) -> HmacBackupReceiptVerifier:
    return HmacBackupReceiptVerifier(key)


def _verify(raw: str | bytes):
    return _verifier().verify(
        raw,
        expected_plan_id=PLAN_ID,
        expected_database_fingerprint=DATABASE_FINGERPRINT,
        expected_alembic_revision=ALEMBIC_REVISION,
        expected_checkpoint_revision=CHECKPOINT_REVISION,
        plan_created_at=PLAN_CREATED_AT,
    )


def _assert_error(raw: str | bytes, code: str) -> BackupReceiptError:
    with pytest.raises(BackupReceiptError) as caught:
        _verify(raw)
    assert caught.value.code == code
    assert str(caught.value) == code
    return caught.value


def test_create_and_verify_canonical_signed_receipt() -> None:
    raw = create_signed_backup_receipt(
        key=KEY,
        plan_id=PLAN_ID,
        database_fingerprint=DATABASE_FINGERPRINT,
        alembic_revision=ALEMBIC_REVISION,
        checkpoint_revision=CHECKPOINT_REVISION,
        backup_id=BACKUP_ID,
        completed_at=COMPLETED_AT,
        manifest_sha256=MANIFEST_SHA256,
    )

    assert raw == _signed_receipt()
    assert raw == _canonical(json.loads(raw)).decode("utf-8")

    verified = _verify(raw)

    assert verified.version == 1
    assert verified.plan_id == PLAN_ID
    assert verified.database_fingerprint == DATABASE_FINGERPRINT
    assert verified.alembic_revision == ALEMBIC_REVISION
    assert verified.checkpoint_revision == CHECKPOINT_REVISION
    assert verified.backup_id == BACKUP_ID
    assert verified.completed_at == COMPLETED_AT
    assert verified.scope == "full_database"
    assert verified.manifest_sha256 == MANIFEST_SHA256
    assert verified.status == "completed"


def test_verifier_uses_constant_time_signature_comparison() -> None:
    original = hmac.compare_digest
    with patch(
        "src.maintenance.cleanup_backup.hmac.compare_digest",
        wraps=original,
    ) as compare_digest:
        _verify(_signed_receipt())

    assert any(
        call.args == (_signed_receipt_signature(), _signed_receipt_signature())
        for call in compare_digest.call_args_list
    )


def _signed_receipt_signature() -> str:
    return json.loads(_signed_receipt())["signature"]


@pytest.mark.parametrize("key", [b"", b"k" * 31, "k" * 32, bytearray(b"k" * 32)])
def test_hmac_key_must_be_at_least_32_bytes(key: object) -> None:
    with pytest.raises(BackupReceiptError) as caught:
        HmacBackupReceiptVerifier(key)  # type: ignore[arg-type]

    assert caught.value.code == "backup_receipt_key_invalid"
    assert str(caught.value) == "backup_receipt_key_invalid"


def test_receipt_rejects_duplicate_keys() -> None:
    raw = _signed_receipt()
    duplicate = raw.replace('"version":1', '"version":1,"version":1')

    _assert_error(duplicate, "backup_receipt_duplicate_key")


@pytest.mark.parametrize(
    ("raw", "code"),
    [
        ("", "backup_receipt_malformed"),
        ("not-json", "backup_receipt_malformed"),
        ("[]", "backup_receipt_schema_invalid"),
        (b"\xff", "backup_receipt_malformed"),
    ],
)
def test_receipt_rejects_malformed_or_non_object_json(
    raw: str | bytes,
    code: str,
) -> None:
    _assert_error(raw, code)


def test_receipt_rejects_oversized_json_before_parsing() -> None:
    raw = b"{" + (b" " * MAX_BACKUP_RECEIPT_BYTES) + b"}"

    _assert_error(raw, "backup_receipt_too_large")


@pytest.mark.parametrize("field", ["plan_id", "signature", "completed_at"])
def test_receipt_rejects_missing_fields(field: str) -> None:
    payload = json.loads(_signed_receipt())
    del payload[field]

    _assert_error(
        _canonical(payload).decode("utf-8"),
        "backup_receipt_schema_invalid",
    )


def test_receipt_rejects_unknown_fields_even_when_signature_is_valid() -> None:
    raw = _signed_receipt(unexpected="data")

    _assert_error(raw, "backup_receipt_schema_invalid")


def test_receipt_must_use_canonical_json() -> None:
    payload = json.loads(_signed_receipt())
    noncanonical = json.dumps(payload, indent=2)

    _assert_error(noncanonical, "backup_receipt_not_canonical")


@pytest.mark.parametrize("version", [0, 2, True, "1"])
def test_receipt_rejects_wrong_version(version: object) -> None:
    _assert_error(
        _signed_receipt(version=version),
        "backup_receipt_version_invalid",
    )


@pytest.mark.parametrize("scope", ["", "checkpoint", "FULL_DATABASE", 1])
def test_receipt_rejects_wrong_scope(scope: object) -> None:
    _assert_error(_signed_receipt(scope=scope), "backup_receipt_scope_invalid")


@pytest.mark.parametrize("status", ["", "started", "failed", 1])
def test_receipt_rejects_non_completed_status(status: object) -> None:
    _assert_error(_signed_receipt(status=status), "backup_receipt_status_invalid")


@pytest.mark.parametrize("backup_id", ["", " ", 1, None])
def test_receipt_rejects_empty_or_non_string_backup_id(backup_id: object) -> None:
    _assert_error(
        _signed_receipt(backup_id=backup_id),
        "backup_receipt_backup_id_invalid",
    )


@pytest.mark.parametrize(
    "manifest",
    ["a" * 63, "a" * 65, "A" * 64, "g" * 64, 1, None],
)
def test_receipt_rejects_invalid_manifest_sha256(manifest: object) -> None:
    _assert_error(
        _signed_receipt(manifest_sha256=manifest),
        "backup_receipt_manifest_invalid",
    )


@pytest.mark.parametrize(
    "completed_at",
    [
        "2026-07-12T01:05:00",
        "2026-07-12T09:05:00+08:00",
        "not-a-time",
        1,
        None,
    ],
)
def test_receipt_completed_at_must_be_an_aware_utc_timestamp(
    completed_at: object,
) -> None:
    _assert_error(
        _signed_receipt(completed_at=completed_at),
        "backup_receipt_completed_at_invalid",
    )


def test_receipt_accepts_explicit_zero_utc_offset() -> None:
    verified = _verify(_signed_receipt(completed_at="2026-07-12T01:05:00+00:00"))

    assert verified.completed_at == COMPLETED_AT


def test_receipt_rejects_completion_before_plan_creation() -> None:
    raw = _signed_receipt(completed_at="2026-07-12T00:59:59Z")

    _assert_error(raw, "backup_receipt_completed_before_plan")


@pytest.mark.parametrize(
    "plan_created_at",
    [
        datetime(2026, 7, 12, 1, 0),
        datetime(2026, 7, 12, 9, 0, tzinfo=timezone(timedelta(hours=8))),
    ],
)
def test_plan_created_at_must_also_be_aware_utc(
    plan_created_at: datetime,
) -> None:
    with pytest.raises(BackupReceiptError) as caught:
        _verifier().verify(
            _signed_receipt(),
            expected_plan_id=PLAN_ID,
            expected_database_fingerprint=DATABASE_FINGERPRINT,
            expected_alembic_revision=ALEMBIC_REVISION,
            expected_checkpoint_revision=CHECKPOINT_REVISION,
            plan_created_at=plan_created_at,
        )

    assert caught.value.code == "backup_receipt_plan_created_at_invalid"


def test_receipt_rejects_wrong_plan_binding_with_a_valid_signature() -> None:
    raw = _signed_receipt(plan_id="3" * 64)

    _assert_error(raw, "backup_receipt_plan_mismatch")


def test_receipt_rejects_wrong_database_binding_with_a_valid_signature() -> None:
    raw = _signed_receipt(database_fingerprint="4" * 64)

    _assert_error(raw, "backup_receipt_database_mismatch")


def test_receipt_rejects_wrong_schema_bindings_with_valid_signatures() -> None:
    _assert_error(
        _signed_receipt(alembic_revision="different_revision"),
        "backup_receipt_alembic_revision_mismatch",
    )
    _assert_error(
        _signed_receipt(checkpoint_revision=8),
        "backup_receipt_checkpoint_revision_mismatch",
    )


@pytest.mark.parametrize("plan_id", ["plan-id", "A" * 64, "g" * 64, "0" * 63])
def test_receipt_requires_sha256_plan_identifier(plan_id: object) -> None:
    _assert_error(
        _signed_receipt(plan_id=plan_id),
        "backup_receipt_plan_invalid",
    )


@pytest.mark.parametrize(
    "database_fingerprint",
    ["database", "A" * 64, "g" * 64, "0" * 65],
)
def test_receipt_requires_sha256_database_fingerprint(
    database_fingerprint: object,
) -> None:
    _assert_error(
        _signed_receipt(database_fingerprint=database_fingerprint),
        "backup_receipt_database_invalid",
    )


@pytest.mark.parametrize("backup_id", [" backup-1", "backup-1 ", "bad\nvalue", "bad\x00value"])
def test_receipt_rejects_noncanonical_or_controlled_backup_ids(
    backup_id: object,
) -> None:
    _assert_error(
        _signed_receipt(backup_id=backup_id),
        "backup_receipt_backup_id_invalid",
    )


@pytest.mark.parametrize("signature", ["", "A" * 64, "g" * 64, "0" * 63, 1])
def test_receipt_rejects_malformed_signature(signature: object) -> None:
    payload = json.loads(_signed_receipt())
    payload["signature"] = signature

    _assert_error(
        _canonical(payload).decode("utf-8"),
        "backup_receipt_signature_invalid",
    )


def test_receipt_rejects_a_well_formed_but_incorrect_signature() -> None:
    payload = json.loads(_signed_receipt())
    payload["signature"] = "0" * 64

    _assert_error(
        _canonical(payload).decode("utf-8"),
        "backup_receipt_signature_invalid",
    )


def test_error_never_echoes_payload_path_secret_or_parser_exception() -> None:
    sensitive = '/private/backup/secret.json api-key="top-secret" {'

    caught = _assert_error(sensitive, "backup_receipt_malformed")

    rendered = f"{caught!s} {caught!r}"
    assert "private" not in rendered
    assert "secret" not in rendered
    assert "api-key" not in rendered
    assert caught.__cause__ is None


def test_generator_rejects_invalid_inputs_with_safe_error_codes() -> None:
    with pytest.raises(BackupReceiptError) as caught:
        create_signed_backup_receipt(
            key=KEY,
            plan_id=PLAN_ID,
            database_fingerprint=DATABASE_FINGERPRINT,
            alembic_revision=ALEMBIC_REVISION,
            checkpoint_revision=CHECKPOINT_REVISION,
            backup_id=" ",
            completed_at=COMPLETED_AT,
            manifest_sha256=MANIFEST_SHA256,
        )

    assert caught.value.code == "backup_receipt_backup_id_invalid"
    assert str(caught.value) == "backup_receipt_backup_id_invalid"
