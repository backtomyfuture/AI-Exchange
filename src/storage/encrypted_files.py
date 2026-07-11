from __future__ import annotations

import asyncio
import base64
import binascii
import hashlib
import hmac
import os
import stat
from pathlib import Path
from typing import Any, Mapping
from uuid import uuid4

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from src.storage.content_store import (
    ContentRef,
    ContentStoreConfigurationError,
    ContentStoreFormatError,
    ContentStoreIntegrityError,
    ContentStoreNotFoundError,
    ContentStoreReferenceError,
    ContentStoreWriteError,
    deserialize_email_envelope,
    serialize_email_envelope,
    validate_key_version,
)


_CIPHERTEXT_MAGIC = b"AIXE1"
_NONCE_BYTES = 12
_TAG_BYTES = 16


class EncryptedFileContentStore:
    """Filesystem-backed content store configured with one AES-256 key version."""

    def __init__(self, *, root: str | Path, key: str, key_version: str) -> None:
        try:
            validated_version = validate_key_version(key_version)
        except ContentStoreReferenceError as exc:
            raise ContentStoreConfigurationError("invalid_key_version") from exc

        try:
            if not isinstance(key, str):
                raise ValueError
            decoded_key = base64.b64decode(key.encode("ascii"), validate=True)
        except (ValueError, UnicodeEncodeError, binascii.Error) as exc:
            raise ContentStoreConfigurationError("invalid_content_store_key") from exc
        if len(decoded_key) != 32:
            raise ContentStoreConfigurationError("invalid_content_store_key")

        try:
            validated_root = Path(root)
        except (TypeError, ValueError):
            raise ContentStoreConfigurationError("invalid_content_store_root") from None
        if (
            not validated_root.is_absolute()
            or validated_root == Path(validated_root.anchor)
            or ".." in validated_root.parts
        ):
            raise ContentStoreConfigurationError("invalid_content_store_root")

        self._root = validated_root
        self._key = decoded_key
        self._key_version = validated_version

    @staticmethod
    def _validate_ref(ref: ContentRef) -> ContentRef:
        if not isinstance(ref, ContentRef):
            raise ContentStoreReferenceError("invalid_content_ref")
        return ContentRef(
            account_id=ref.account_id,
            object_id=ref.object_id,
            key_version=ref.key_version,
            sha256=ref.sha256,
        )

    def _path_for_ref(self, ref: ContentRef) -> Path:
        validated = self._validate_ref(ref)
        return self._root / str(validated.account_id) / f"{validated.object_id}.enc"

    @staticmethod
    def _aad(ref: ContentRef) -> bytes:
        return f"{ref.account_id}:{ref.object_id}:{ref.key_version}".encode()

    @staticmethod
    def _ensure_private_directory(path: Path) -> None:
        try:
            path.mkdir(mode=0o700, parents=True, exist_ok=True)
            if not stat.S_ISDIR(path.lstat().st_mode):
                raise OSError
            os.chmod(path, 0o700)
        except Exception:
            raise ContentStoreWriteError("content_write_failed") from None

    @staticmethod
    def _fsync_directory(path: Path) -> None:
        flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        directory_fd = os.open(path, flags)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)

    def _write_blob_atomic(self, path: Path, blob: bytes) -> None:
        self._ensure_private_directory(self._root)
        self._ensure_private_directory(path.parent)
        temp_path = path.with_name(f".{uuid4().hex}.{uuid4().hex}.tmp")
        raw_fd: int | None = None
        replaced = False
        try:
            raw_fd = os.open(
                temp_path,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                0o600,
            )
            os.chmod(temp_path, 0o600)
            with os.fdopen(raw_fd, "wb") as handle:
                raw_fd = None
                handle.write(blob)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp_path, path)
            replaced = True
            os.chmod(path, 0o600)
            self._fsync_directory(path.parent)
        except Exception:
            if raw_fd is not None:
                try:
                    os.close(raw_fd)
                except OSError:
                    pass
            if not replaced:
                try:
                    temp_path.unlink(missing_ok=True)
                except OSError:
                    pass
            raise ContentStoreWriteError("content_write_failed") from None

    def _require_known_version(self, ref: ContentRef) -> ContentRef:
        validated_ref = self._validate_ref(ref)
        if validated_ref.key_version != self._key_version:
            raise ContentStoreFormatError("unknown_key_version")
        return validated_ref

    def _put_email_sync(
        self,
        account_id: int,
        email_id: str,
        email: Mapping[str, Any],
    ) -> ContentRef:
        if type(account_id) is not int or account_id <= 0:
            raise ContentStoreReferenceError("invalid_content_ref")
        plaintext = serialize_email_envelope(email_id, email)
        ref = ContentRef(
            account_id=account_id,
            object_id=str(uuid4()),
            key_version=self._key_version,
            sha256=hashlib.sha256(plaintext).hexdigest(),
        )
        nonce = os.urandom(_NONCE_BYTES)
        ciphertext = AESGCM(self._key).encrypt(nonce, plaintext, self._aad(ref))
        blob = _CIPHERTEXT_MAGIC + nonce + ciphertext
        path = self._path_for_ref(ref)
        self._write_blob_atomic(path, blob)
        return ref

    async def put_email(
        self,
        account_id: int,
        email_id: str,
        email: Mapping[str, Any],
    ) -> ContentRef:
        return await asyncio.to_thread(
            self._put_email_sync,
            account_id,
            email_id,
            email,
        )

    def _load_email_sync(
        self,
        ref: ContentRef,
        include_attachments: bool = False,
    ) -> dict[str, Any]:
        validated_ref = self._require_known_version(ref)
        path = self._path_for_ref(validated_ref)
        try:
            blob = path.read_bytes()
        except FileNotFoundError:
            raise ContentStoreNotFoundError("content_not_found") from None
        except OSError:
            raise ContentStoreFormatError("content_read_failed") from None

        minimum_length = len(_CIPHERTEXT_MAGIC) + _NONCE_BYTES + _TAG_BYTES
        if len(blob) < minimum_length or not blob.startswith(_CIPHERTEXT_MAGIC):
            raise ContentStoreFormatError("invalid_ciphertext_file")
        nonce_start = len(_CIPHERTEXT_MAGIC)
        nonce_end = nonce_start + _NONCE_BYTES
        nonce = blob[nonce_start:nonce_end]
        ciphertext = blob[nonce_end:]
        try:
            plaintext = AESGCM(self._key).decrypt(
                nonce,
                ciphertext,
                self._aad(validated_ref),
            )
        except InvalidTag:
            raise ContentStoreIntegrityError("content_authentication_failed") from None

        actual_hash = hashlib.sha256(plaintext).hexdigest()
        if not hmac.compare_digest(actual_hash, validated_ref.sha256):
            raise ContentStoreIntegrityError("content_hash_mismatch")
        return deserialize_email_envelope(
            plaintext,
            include_attachments=include_attachments,
        )

    async def load_email(
        self,
        ref: ContentRef,
        *,
        include_attachments: bool = False,
    ) -> dict[str, Any]:
        return await asyncio.to_thread(
            self._load_email_sync,
            ref,
            include_attachments,
        )

    def _delete_sync(self, ref: ContentRef) -> None:
        validated_ref = self._require_known_version(ref)
        path = self._path_for_ref(validated_ref)
        try:
            path.unlink()
        except FileNotFoundError:
            return
        except OSError:
            raise ContentStoreWriteError("content_delete_failed") from None
        try:
            self._fsync_directory(path.parent)
        except Exception:
            raise ContentStoreWriteError("content_delete_failed") from None

    async def delete(self, ref: ContentRef) -> None:
        await asyncio.to_thread(self._delete_sync, ref)
