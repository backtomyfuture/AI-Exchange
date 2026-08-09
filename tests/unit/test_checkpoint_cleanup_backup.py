from __future__ import annotations

import json
from base64 import urlsafe_b64decode
from datetime import datetime, timezone

import pytest
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

from src.maintenance.cleanup_backup import (
    ED25519_BACKUP_RECEIPT_VERSION,
    ED25519_SIGNATURE_ALGORITHM,
    BackupReceiptError,
    Ed25519BackupReceiptVerifier,
    create_ed25519_signed_backup_receipt,
)


PLAN_ID = "1" * 64
DATABASE_FINGERPRINT = "2" * 64
BACKUP_ID = "backup-20260808-001"
MANIFEST_SHA256 = "a" * 64
ALEMBIC_REVISION = "20260808_0001"
CHECKPOINT_REVISION = 9
PLAN_CREATED_AT = datetime(2026, 7, 12, 1, 0, tzinfo=timezone.utc)
COMPLETED_AT = datetime(2026, 7, 12, 1, 5, tzinfo=timezone.utc)
PRIVATE_SEED = bytes.fromhex(
    "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60"
)
PUBLIC_KEY = bytes.fromhex(
    "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a"
)


def _canonical(payload: dict[str, object]) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _receipt() -> str:
    return create_ed25519_signed_backup_receipt(
        private_seed=PRIVATE_SEED,
        plan_id=PLAN_ID,
        database_fingerprint=DATABASE_FINGERPRINT,
        alembic_revision=ALEMBIC_REVISION,
        checkpoint_revision=CHECKPOINT_REVISION,
        backup_id=BACKUP_ID,
        completed_at=COMPLETED_AT,
        manifest_sha256=MANIFEST_SHA256,
    )


def _verify(raw: str):
    return Ed25519BackupReceiptVerifier(PUBLIC_KEY).verify(
        raw,
        expected_plan_id=PLAN_ID,
        expected_database_fingerprint=DATABASE_FINGERPRINT,
        expected_alembic_revision=ALEMBIC_REVISION,
        expected_checkpoint_revision=CHECKPOINT_REVISION,
        plan_created_at=PLAN_CREATED_AT,
    )


def test_create_and_verify_only_canonical_ed25519_v2_receipt() -> None:
    raw = _receipt()
    payload = json.loads(raw)

    assert raw == _canonical(payload).decode()
    assert payload["version"] == ED25519_BACKUP_RECEIPT_VERSION == 2
    assert payload["signature_algorithm"] == ED25519_SIGNATURE_ALGORITHM
    assert _verify(raw).backup_id == BACKUP_ID


def test_signature_covers_every_unsigned_claim() -> None:
    payload = json.loads(_receipt())
    signature = urlsafe_b64decode(f"{payload.pop('signature')}==")
    key = Ed25519PublicKey.from_public_bytes(PUBLIC_KEY)
    key.verify(signature, _canonical(payload))
    payload["backup_id"] = "tampered"
    with pytest.raises(InvalidSignature):
        key.verify(signature, _canonical(payload))


def test_v1_receipt_is_rejected_without_compatibility_verifier() -> None:
    payload = json.loads(_receipt())
    payload["version"] = 1
    with pytest.raises(BackupReceiptError, match="backup_receipt_version_invalid"):
        _verify(_canonical(payload).decode())


@pytest.mark.parametrize("key", [b"", b"k" * 31, b"k" * 33])
def test_verifier_rejects_invalid_public_key(key: bytes) -> None:
    with pytest.raises(BackupReceiptError, match="backup_receipt_key_invalid"):
        Ed25519BackupReceiptVerifier(key)


def test_noncanonical_or_modified_receipt_is_rejected() -> None:
    with pytest.raises(BackupReceiptError, match="backup_receipt_not_canonical"):
        _verify(json.dumps(json.loads(_receipt()), indent=2))
    payload = json.loads(_receipt())
    payload["signature"] = "A" * 86
    with pytest.raises(BackupReceiptError, match="backup_receipt_signature_invalid"):
        _verify(_canonical(payload).decode())
