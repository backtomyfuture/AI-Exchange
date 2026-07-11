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
_DIRECTORY_FLAGS = (
    os.O_RDONLY
    | getattr(os, "O_DIRECTORY", 0)
    | getattr(os, "O_CLOEXEC", 0)
    | getattr(os, "O_NOFOLLOW", 0)
)
_READ_FLAGS = (
    os.O_RDONLY
    | getattr(os, "O_CLOEXEC", 0)
    | getattr(os, "O_NOFOLLOW", 0)
)


class _UnsafeStoragePathError(Exception):
    pass


class _StorageObjectMissing(Exception):
    pass


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
        except (ValueError, UnicodeEncodeError, binascii.Error):
            raise ContentStoreConfigurationError("invalid_content_store_key") from None
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
        self._preflight_root()

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
    def _close_fd(fd: int | None) -> None:
        if fd is not None:
            try:
                os.close(fd)
            except OSError:
                pass

    @staticmethod
    def _validate_private_directory_fd(fd: int) -> None:
        info = os.fstat(fd)
        if (
            not stat.S_ISDIR(info.st_mode)
            or info.st_uid != os.geteuid()
            or stat.S_IMODE(info.st_mode) != 0o700
        ):
            raise _UnsafeStoragePathError

    @staticmethod
    def _verify_directory_link(parent_fd: int, name: str, directory_fd: int) -> None:
        linked = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        opened = os.fstat(directory_fd)
        if (
            not stat.S_ISDIR(linked.st_mode)
            or (linked.st_dev, linked.st_ino) != (opened.st_dev, opened.st_ino)
        ):
            raise _UnsafeStoragePathError

    @staticmethod
    def _validate_regular_file_fd(fd: int) -> None:
        info = os.fstat(fd)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.geteuid()
            or stat.S_IMODE(info.st_mode) != 0o600
        ):
            raise _UnsafeStoragePathError

    @staticmethod
    def _verify_regular_file_link(directory_fd: int, name: str, file_fd: int) -> None:
        linked = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        opened = os.fstat(file_fd)
        if (
            not stat.S_ISREG(linked.st_mode)
            or (linked.st_dev, linked.st_ino) != (opened.st_dev, opened.st_ino)
        ):
            raise _UnsafeStoragePathError

    def _open_root_parent(self) -> tuple[int, str]:
        parts = self._root.parts
        current_fd: int | None = None
        try:
            current_fd = os.open(parts[0], _DIRECTORY_FLAGS)
            for part in parts[1:-1]:
                next_fd = os.open(part, _DIRECTORY_FLAGS, dir_fd=current_fd)
                os.close(current_fd)
                current_fd = next_fd
            return current_fd, parts[-1]
        except Exception:
            self._close_fd(current_fd)
            raise _UnsafeStoragePathError from None

    def _open_root_directory(self, *, create: bool) -> tuple[int, int, str]:
        parent_fd, root_name = self._open_root_parent()
        root_fd: int | None = None
        created = False
        try:
            if create:
                try:
                    os.mkdir(root_name, mode=0o700, dir_fd=parent_fd)
                    created = True
                except FileExistsError:
                    pass
            try:
                root_fd = os.open(root_name, _DIRECTORY_FLAGS, dir_fd=parent_fd)
            except FileNotFoundError:
                raise _StorageObjectMissing from None
            if created:
                os.fchmod(root_fd, 0o700)
                os.fsync(parent_fd)
            self._validate_private_directory_fd(root_fd)
            self._verify_directory_link(parent_fd, root_name, root_fd)
            return parent_fd, root_fd, root_name
        except (_StorageObjectMissing, _UnsafeStoragePathError):
            self._close_fd(root_fd)
            self._close_fd(parent_fd)
            raise
        except Exception:
            self._close_fd(root_fd)
            self._close_fd(parent_fd)
            raise _UnsafeStoragePathError from None

    def _open_account_directory(
        self,
        root_parent_fd: int,
        root_fd: int,
        root_name: str,
        account_id: int,
        *,
        create: bool,
    ) -> int:
        account_name = str(account_id)
        account_fd: int | None = None
        created = False
        try:
            self._verify_directory_link(root_parent_fd, root_name, root_fd)
            if create:
                try:
                    os.mkdir(account_name, mode=0o700, dir_fd=root_fd)
                    created = True
                except FileExistsError:
                    pass
            try:
                account_fd = os.open(account_name, _DIRECTORY_FLAGS, dir_fd=root_fd)
            except FileNotFoundError:
                raise _StorageObjectMissing from None
            if created:
                os.fchmod(account_fd, 0o700)
                os.fsync(root_fd)
            self._validate_private_directory_fd(account_fd)
            self._verify_directory_link(root_fd, account_name, account_fd)
            return account_fd
        except (_StorageObjectMissing, _UnsafeStoragePathError):
            self._close_fd(account_fd)
            raise
        except Exception:
            self._close_fd(account_fd)
            raise _UnsafeStoragePathError from None

    def _preflight_root(self) -> None:
        try:
            parent_fd, root_fd, _root_name = self._open_root_directory(create=False)
        except _StorageObjectMissing:
            return
        except Exception:
            raise ContentStoreConfigurationError("invalid_content_store_root") from None
        self._close_fd(root_fd)
        self._close_fd(parent_fd)

    def _write_blob_atomic(self, account_id: int, final_name: str, blob: bytes) -> None:
        root_parent_fd: int | None = None
        root_fd: int | None = None
        root_name: str | None = None
        account_fd: int | None = None
        temp_name = f".{uuid4().hex}.{uuid4().hex}.tmp"
        raw_fd: int | None = None
        replaced = False
        try:
            root_parent_fd, root_fd, root_name = self._open_root_directory(create=True)
            account_fd = self._open_account_directory(
                root_parent_fd,
                root_fd,
                root_name,
                account_id,
                create=True,
            )
            raw_fd = os.open(
                temp_name,
                os.O_WRONLY
                | os.O_CREAT
                | os.O_EXCL
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0),
                0o600,
                dir_fd=account_fd,
            )
            os.fchmod(raw_fd, 0o600)
            with os.fdopen(raw_fd, "wb") as handle:
                raw_fd = None
                handle.write(blob)
                handle.flush()
                os.fsync(handle.fileno())
            self._verify_directory_link(root_parent_fd, root_name, root_fd)
            self._verify_directory_link(root_fd, str(account_id), account_fd)
            os.replace(
                temp_name,
                final_name,
                src_dir_fd=account_fd,
                dst_dir_fd=account_fd,
            )
            replaced = True
            try:
                self._verify_directory_link(root_parent_fd, root_name, root_fd)
                self._verify_directory_link(root_fd, str(account_id), account_fd)
            except Exception:
                try:
                    os.unlink(final_name, dir_fd=account_fd)
                    os.fsync(account_fd)
                except OSError:
                    pass
                raise _UnsafeStoragePathError from None
            os.fsync(account_fd)
        except Exception:
            if raw_fd is not None:
                self._close_fd(raw_fd)
            if not replaced:
                try:
                    if account_fd is not None:
                        os.unlink(temp_name, dir_fd=account_fd)
                except OSError:
                    pass
            raise ContentStoreWriteError("content_write_failed") from None
        finally:
            self._close_fd(account_fd)
            self._close_fd(root_fd)
            self._close_fd(root_parent_fd)

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
        self._write_blob_atomic(
            ref.account_id,
            f"{ref.object_id}.enc",
            blob,
        )
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
        root_parent_fd: int | None = None
        root_fd: int | None = None
        root_name: str | None = None
        account_fd: int | None = None
        file_fd: int | None = None
        final_name = f"{validated_ref.object_id}.enc"
        try:
            root_parent_fd, root_fd, root_name = self._open_root_directory(create=False)
            account_fd = self._open_account_directory(
                root_parent_fd,
                root_fd,
                root_name,
                validated_ref.account_id,
                create=False,
            )
            try:
                file_fd = os.open(
                    final_name,
                    _READ_FLAGS,
                    dir_fd=account_fd,
                )
            except FileNotFoundError:
                raise _StorageObjectMissing from None
            self._validate_regular_file_fd(file_fd)
            self._verify_regular_file_link(account_fd, final_name, file_fd)
            chunks: list[bytes] = []
            while chunk := os.read(file_fd, 1024 * 1024):
                chunks.append(chunk)
            self._verify_directory_link(root_parent_fd, root_name, root_fd)
            self._verify_directory_link(
                root_fd,
                str(validated_ref.account_id),
                account_fd,
            )
            self._verify_regular_file_link(account_fd, final_name, file_fd)
            blob = b"".join(chunks)
        except _StorageObjectMissing:
            raise ContentStoreNotFoundError("content_not_found") from None
        except Exception:
            raise ContentStoreFormatError("content_read_failed") from None
        finally:
            self._close_fd(file_fd)
            self._close_fd(account_fd)
            self._close_fd(root_fd)
            self._close_fd(root_parent_fd)

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
        root_parent_fd: int | None = None
        root_fd: int | None = None
        root_name: str | None = None
        account_fd: int | None = None
        file_fd: int | None = None
        final_name = f"{validated_ref.object_id}.enc"
        try:
            root_parent_fd, root_fd, root_name = self._open_root_directory(create=False)
            account_fd = self._open_account_directory(
                root_parent_fd,
                root_fd,
                root_name,
                validated_ref.account_id,
                create=False,
            )
            try:
                file_fd = os.open(final_name, _READ_FLAGS, dir_fd=account_fd)
            except FileNotFoundError:
                raise _StorageObjectMissing from None
            self._validate_regular_file_fd(file_fd)
            self._verify_regular_file_link(account_fd, final_name, file_fd)
            self._verify_directory_link(root_parent_fd, root_name, root_fd)
            self._verify_directory_link(
                root_fd,
                str(validated_ref.account_id),
                account_fd,
            )
            self._close_fd(file_fd)
            file_fd = None
            os.unlink(final_name, dir_fd=account_fd)
            os.fsync(account_fd)
        except _StorageObjectMissing:
            return
        except Exception:
            raise ContentStoreWriteError("content_delete_failed") from None
        finally:
            self._close_fd(file_fd)
            self._close_fd(account_fd)
            self._close_fd(root_fd)
            self._close_fd(root_parent_fd)

    async def delete(self, ref: ContentRef) -> None:
        await asyncio.to_thread(self._delete_sync, ref)
