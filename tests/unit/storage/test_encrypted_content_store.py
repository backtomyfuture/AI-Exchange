from __future__ import annotations

import base64
import hashlib
import hmac
import inspect
import json
import os
import stat
import struct
import time
from copy import deepcopy
from dataclasses import FrozenInstanceError, replace
from pathlib import Path
from uuid import uuid4

import pytest
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from collections.abc import Mapping


def _valid_ref(**overrides):
    from src.storage import ContentRef

    values = {
        "account_id": 8,
        "object_id": str(uuid4()),
        "key_version": "v1",
        "sha256": "0" * 64,
    }
    values.update(overrides)
    return ContentRef(**values)


def test_content_ref_is_immutable_and_protocol_signatures_are_locked():
    from src.storage import ContentRef, ContentStore

    ref = _valid_ref()
    with pytest.raises(FrozenInstanceError):
        ref.account_id = 9

    assert list(inspect.signature(ContentStore.put_email).parameters) == [
        "self",
        "account_id",
        "email_id",
        "email",
    ]
    assert list(inspect.signature(ContentStore.load_email).parameters) == [
        "self",
        "ref",
        "include_attachments",
    ]
    assert list(inspect.signature(ContentStore.delete).parameters) == ["self", "ref"]
    assert ContentRef.__dataclass_params__.frozen is True


@pytest.mark.parametrize("account_id", [True, False, 0, -1, 1.0, "8"])
def test_content_ref_rejects_non_positive_or_bool_account_ids(account_id):
    from src.storage import ContentStoreReferenceError

    with pytest.raises(ContentStoreReferenceError, match="invalid_content_ref"):
        _valid_ref(account_id=account_id)


@pytest.mark.parametrize(
    "object_id",
    [
        "not-a-uuid",
        "../escape",
        uuid4().hex,
        str(uuid4()).upper(),
    ],
)
def test_content_ref_requires_a_canonical_uuid(object_id):
    from src.storage import ContentStoreReferenceError

    with pytest.raises(ContentStoreReferenceError, match="invalid_content_ref"):
        _valid_ref(object_id=object_id)


@pytest.mark.parametrize("key_version", ["", "../v1", "v 1", "密钥", "a" * 65])
def test_content_ref_rejects_unsafe_key_versions(key_version):
    from src.storage import ContentStoreReferenceError

    with pytest.raises(ContentStoreReferenceError, match="invalid_content_ref"):
        _valid_ref(key_version=key_version)


@pytest.mark.parametrize("sha256", ["0" * 63, "0" * 65, "A" * 64, "g" * 64])
def test_content_ref_requires_lowercase_sha256(sha256):
    from src.storage import ContentStoreReferenceError

    with pytest.raises(ContentStoreReferenceError, match="invalid_content_ref"):
        _valid_ref(sha256=sha256)


@pytest.mark.parametrize(
    "key",
    [
        "",
        "not base64!",
        base64.b64encode(b"short").decode("ascii"),
        base64.b64encode(b"x" * 33).decode("ascii"),
    ],
)
def test_store_rejects_missing_bad_base64_and_wrong_length_keys(root, key):
    from src.storage import ContentStoreConfigurationError, EncryptedFileContentStore

    with pytest.raises(ContentStoreConfigurationError, match="invalid_content_store_key"):
        EncryptedFileContentStore(root=root, key=key, key_version="v1")
    assert not root.exists()


def test_store_rejects_unsafe_current_key_version_before_touching_disk(root, valid_key):
    from src.storage import ContentStoreConfigurationError, EncryptedFileContentStore

    with pytest.raises(ContentStoreConfigurationError, match="invalid_key_version"):
        EncryptedFileContentStore(root=root, key=valid_key, key_version="../v1")
    assert not root.exists()


@pytest.mark.parametrize("unsafe_root", [Path("relative/content"), Path("/")])
def test_store_rejects_unsafe_content_roots_before_touching_disk(
    unsafe_root,
    valid_key,
):
    from src.storage import ContentStoreConfigurationError, EncryptedFileContentStore

    with pytest.raises(ContentStoreConfigurationError, match="invalid_content_store_root"):
        EncryptedFileContentStore(
            root=unsafe_root,
            key=valid_key,
            key_version="v1",
        )


def test_content_store_settings_have_fail_closed_defaults(monkeypatch):
    from src.config import Settings, resolve_secret

    monkeypatch.delenv("CONTENT_STORE_ROOT", raising=False)
    monkeypatch.delenv("CONTENT_STORE_KEY", raising=False)
    monkeypatch.delenv("CONTENT_STORE_KEY_VERSION", raising=False)
    settings = Settings(_env_file=None)

    assert settings.CONTENT_STORE_ROOT == "/app/data/content"
    assert resolve_secret(settings.CONTENT_STORE_KEY) == ""
    assert settings.CONTENT_STORE_KEY_VERSION == "v1"


def test_aixc1_envelope_is_deterministic_and_stores_raw_attachment_segments():
    from src.storage.content_store import serialize_email_envelope

    first_bytes = b"raw-secret-one"
    second_bytes = b"\x00\xffraw-secret-two"
    first_b64 = base64.b64encode(first_bytes).decode("ascii")
    second_b64 = base64.b64encode(second_bytes).decode("ascii")
    email = {
        "id": "mail-1",
        "subject": "subject",
        "body": "body",
        "attachments": [
            {"name": "one.bin", "content_type": "application/octet-stream", "content": first_b64},
            {"name": "metadata-only.txt", "size": 0},
            {"name": "two.bin", "content": second_b64},
        ],
    }
    original = deepcopy(email)

    first = serialize_email_envelope("mail-1", email)
    second = serialize_email_envelope("mail-1", email)

    assert first == second
    assert first.startswith(b"AIXC1")
    header_length = struct.unpack(">Q", first[5:13])[0]
    header_bytes = first[13 : 13 + header_length]
    assert first[13 + header_length :] == first_bytes + second_bytes
    assert first_b64.encode("ascii") not in header_bytes
    assert second_b64.encode("ascii") not in header_bytes
    assert email == original


def test_aixc1_load_shapes_restore_only_explicit_attachment_content():
    from src.storage.content_store import deserialize_email_envelope, serialize_email_envelope

    email = {
        "body": "secret-body",
        "attachments": [
            {"name": "with.bin", "content": base64.b64encode(b"payload").decode("ascii")},
            {"name": "without.bin", "size": 12},
            {"name": "empty.bin", "content": ""},
        ],
    }
    original = deepcopy(email)
    envelope = serialize_email_envelope("mail-2", email)

    metadata_only = deserialize_email_envelope(envelope)
    with_content = deserialize_email_envelope(envelope, include_attachments=True)

    assert metadata_only == {
        "body": "secret-body",
        "attachments": [
            {"name": "with.bin"},
            {"name": "without.bin", "size": 12},
            {"name": "empty.bin"},
        ],
    }
    assert with_content["attachments"][0]["content"] == base64.b64encode(b"payload").decode("ascii")
    assert "content" not in with_content["attachments"][1]
    assert with_content["attachments"][2]["content"] == ""
    assert email == original


def test_aixc1_rejects_invalid_attachment_base64_without_exposing_it():
    from src.storage import ContentStoreFormatError
    from src.storage.content_store import serialize_email_envelope

    with pytest.raises(ContentStoreFormatError, match="invalid_email_envelope") as caught:
        serialize_email_envelope(
            "mail-3",
            {"attachments": [{"name": "bad", "content": "not base64!"}]},
        )
    assert "not base64" not in str(caught.value)


def test_aixc1_treats_none_attachment_content_as_metadata_only():
    from src.storage.content_store import deserialize_email_envelope, serialize_email_envelope

    email = {
        "attachments": [
            {"name": "remote.bin", "size": 123, "content": None},
            {"name": "empty.bin", "size": 0, "content": ""},
        ]
    }
    loaded = deserialize_email_envelope(
        serialize_email_envelope("mail-none", email),
        include_attachments=True,
    )

    assert loaded["attachments"] == [
        {"name": "remote.bin", "size": 123},
        {"name": "empty.bin", "size": 0, "content": ""},
    ]


def test_aixc1_normalizes_attachment_mapping_failures_without_leaking_data():
    from src.storage import ContentStoreFormatError
    from src.storage.content_store import serialize_email_envelope

    class ExplodingAttachment(Mapping):
        def __getitem__(self, _key):
            raise RuntimeError("secret-attachment-data")

        def __iter__(self):
            raise RuntimeError("secret-attachment-data")

        def __len__(self):
            return 1

    with pytest.raises(ContentStoreFormatError, match="invalid_email_envelope") as caught:
        serialize_email_envelope(
            "mail-mapping",
            {"attachments": [ExplodingAttachment()]},
        )
    assert "secret-attachment-data" not in str(caught.value)


def _raw_aixc1(header, segments: bytes = b"") -> bytes:
    if isinstance(header, bytes):
        header_bytes = header
    else:
        header_bytes = json.dumps(
            header,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    return b"AIXC1" + struct.pack(">Q", len(header_bytes)) + header_bytes + segments


@pytest.mark.parametrize(
    "malformed",
    [
        b"BAD!!" + struct.pack(">Q", 2) + b"{}",
        b"AIXC1" + struct.pack(">Q", 50) + b"{}",
        _raw_aixc1(b"\xff"),
        _raw_aixc1(b"{"),
        _raw_aixc1([]),
        _raw_aixc1({"email_id": "mail", "email": [], "attachment_segments": []}),
        _raw_aixc1(
            {
                "email_id": "mail",
                "email": {"attachments": [{"name": "x"}]},
                "attachment_segments": [{"has_content": True, "byte_length": 3}],
            },
            b"ab",
        ),
        _raw_aixc1(
            {
                "email_id": "mail",
                "email": {"attachments": []},
                "attachment_segments": [],
            },
            b"trailing",
        ),
    ],
)
def test_aixc1_parser_rejects_malformed_or_inconsistent_envelopes(malformed):
    from src.storage import ContentStoreFormatError
    from src.storage.content_store import deserialize_email_envelope

    with pytest.raises(ContentStoreFormatError, match="invalid_email_envelope"):
        deserialize_email_envelope(malformed, include_attachments=True)


@pytest.mark.asyncio
async def test_content_store_never_writes_plaintext_and_round_trips_shapes(store, root):
    attachment_bytes = b"attachment-secret-bytes"
    attachment_b64 = base64.b64encode(attachment_bytes).decode("ascii")
    email = {
        "body": "secret-body",
        "attachments": [
            {"name": "secret.bin", "content": attachment_b64},
            {"name": "metadata-only.txt", "size": 3},
        ],
    }
    original = deepcopy(email)

    ref = await store.put_email(8, "mail-1", email)
    encrypted_files = list(root.rglob("*.enc"))
    assert len(encrypted_files) == 1
    disk_bytes = encrypted_files[0].read_bytes()

    assert disk_bytes.startswith(b"AIXE1")
    assert b"secret-body" not in disk_bytes
    assert attachment_bytes not in disk_bytes
    assert attachment_b64.encode("ascii") not in disk_bytes
    assert (await store.load_email(ref)) == {
        "body": "secret-body",
        "attachments": [
            {"name": "secret.bin"},
            {"name": "metadata-only.txt", "size": 3},
        ],
    }
    assert (await store.load_email(ref, include_attachments=True)) == original
    assert email == original


@pytest.mark.asyncio
async def test_put_rejects_invalid_account_before_serialization(store, monkeypatch):
    from src.storage import ContentStoreReferenceError, encrypted_files

    serialization_calls = []

    def unexpected_serialization(*_args):
        serialization_calls.append(True)
        raise AssertionError("serialization must not run")

    monkeypatch.setattr(
        encrypted_files,
        "serialize_email_envelope",
        unexpected_serialization,
    )

    with pytest.raises(ContentStoreReferenceError, match="invalid_content_ref"):
        await store.put_email(True, "mail-invalid-account", {"body": "secret"})
    assert serialization_calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("link_level", ["root", "account"])
async def test_store_rejects_symlinked_storage_directories(
    tmp_path,
    valid_key,
    link_level,
):
    from src.storage import ContentStoreWriteError, EncryptedFileContentStore

    outside = tmp_path / "outside"
    outside.mkdir()
    root = tmp_path / "content"
    if link_level == "root":
        root.symlink_to(outside, target_is_directory=True)
    else:
        root.mkdir()
        (root / "8").symlink_to(outside, target_is_directory=True)

    store = EncryptedFileContentStore(root=root, key=valid_key, key_version="v1")
    with pytest.raises(ContentStoreWriteError, match="content_write_failed"):
        await store.put_email(8, "mail-symlink", {"body": "secret"})

    assert not list(outside.rglob("*.enc"))
    assert not list(outside.rglob("*.tmp"))


@pytest.mark.asyncio
async def test_aixe1_uses_exact_aad_and_hashes_complete_plaintext(store, root, valid_key):
    from src.storage.content_store import serialize_email_envelope

    email = {"body": "authenticated", "attachments": []}
    ref = await store.put_email(8, "mail-aad", email)
    blob = (root / "8" / f"{ref.object_id}.enc").read_bytes()
    nonce = blob[5:17]
    ciphertext = blob[17:]
    expected_aad = f"{ref.account_id}:{ref.object_id}:{ref.key_version}".encode()
    plaintext = AESGCM(base64.b64decode(valid_key)).decrypt(
        nonce,
        ciphertext,
        expected_aad,
    )

    assert plaintext == serialize_email_envelope("mail-aad", email)
    assert ref.sha256 == hashlib.sha256(plaintext).hexdigest()


@pytest.mark.asyncio
async def test_ciphertext_or_tag_tampering_fails_closed(store, root):
    from src.storage import ContentStoreIntegrityError

    ref = await store.put_email(8, "mail-tamper", {"body": "secret", "attachments": []})
    path = root / "8" / f"{ref.object_id}.enc"
    tampered = bytearray(path.read_bytes())
    tampered[-1] ^= 1
    path.write_bytes(tampered)

    with pytest.raises(ContentStoreIntegrityError, match="content_authentication_failed") as caught:
        await store.load_email(ref)
    assert "secret" not in str(caught.value)


@pytest.mark.asyncio
async def test_reference_identity_is_authenticated_as_aad(store, root):
    from src.storage import ContentStoreIntegrityError

    ref = await store.put_email(8, "mail-aad-tamper", {"body": "secret", "attachments": []})
    alternate_object_id = str(uuid4())
    alternate_ref = replace(ref, object_id=alternate_object_id)
    source = root / "8" / f"{ref.object_id}.enc"
    target = root / "8" / f"{alternate_object_id}.enc"
    target.write_bytes(source.read_bytes())

    with pytest.raises(ContentStoreIntegrityError, match="content_authentication_failed"):
        await store.load_email(alternate_ref)


@pytest.mark.asyncio
async def test_plaintext_sha_mismatch_uses_constant_time_comparison(store, monkeypatch):
    from src.storage import ContentStoreIntegrityError

    ref = await store.put_email(8, "mail-hash", {"body": "secret", "attachments": []})
    bad_sha = ("1" if ref.sha256[0] != "1" else "0") + ref.sha256[1:]
    compare_calls = []
    original_compare = hmac.compare_digest

    def tracked_compare(left, right):
        compare_calls.append((left, right))
        return original_compare(left, right)

    monkeypatch.setattr("src.storage.encrypted_files.hmac.compare_digest", tracked_compare)
    with pytest.raises(ContentStoreIntegrityError, match="content_hash_mismatch"):
        await store.load_email(replace(ref, sha256=bad_sha))
    assert compare_calls == [(ref.sha256, bad_sha)]


@pytest.mark.asyncio
async def test_unknown_key_version_and_missing_object_fail_closed(store):
    from src.storage import ContentStoreFormatError, ContentStoreNotFoundError

    ref = await store.put_email(8, "mail-version", {"body": "secret", "attachments": []})
    with pytest.raises(ContentStoreFormatError, match="unknown_key_version"):
        await store.load_email(replace(ref, key_version="v2"))

    missing = _valid_ref(sha256=ref.sha256)
    with pytest.raises(ContentStoreNotFoundError, match="content_not_found"):
        await store.load_email(missing)


@pytest.mark.asyncio
@pytest.mark.parametrize("malformed", [b"", b"AIXE1", b"BAD!!" + b"0" * 40])
async def test_malformed_ciphertext_file_fails_closed(store, root, malformed):
    from src.storage import ContentStoreFormatError

    ref = await store.put_email(8, "mail-format", {"body": "secret", "attachments": []})
    path = root / "8" / f"{ref.object_id}.enc"
    path.write_bytes(malformed)

    with pytest.raises(ContentStoreFormatError, match="invalid_ciphertext_file"):
        await store.load_email(ref)


@pytest.mark.asyncio
async def test_atomic_write_uses_exclusive_sibling_temp_fsync_and_replace(
    store,
    root,
    monkeypatch,
):
    from src.storage import encrypted_files

    original_open = os.open
    original_fsync = os.fsync
    original_replace = os.replace
    opened = []
    fsynced_kinds = []
    replacements = []

    def tracked_open(path, flags, mode=0o777, *, dir_fd=None):
        opened.append((os.fspath(path), flags, mode))
        return original_open(path, flags, mode, dir_fd=dir_fd)

    def tracked_fsync(fd):
        mode = os.fstat(fd).st_mode
        fsynced_kinds.append("dir" if stat.S_ISDIR(mode) else "file")
        return original_fsync(fd)

    def tracked_replace(source, target):
        source_path = os.fspath(source)
        target_path = os.fspath(target)
        replacements.append((source_path, target_path))
        assert os.path.dirname(source_path) == os.path.dirname(target_path)
        assert stat.S_IMODE(os.stat(source_path).st_mode) == 0o600
        return original_replace(source, target)

    monkeypatch.setattr(encrypted_files.os, "open", tracked_open)
    monkeypatch.setattr(encrypted_files.os, "fsync", tracked_fsync)
    monkeypatch.setattr(encrypted_files.os, "replace", tracked_replace)

    ref = await store.put_email(8, "mail-atomic", {"body": "secret", "attachments": []})

    temp_opens = [record for record in opened if record[0].endswith(".tmp")]
    assert len(temp_opens) == 1
    assert temp_opens[0][1] & os.O_CREAT
    assert temp_opens[0][1] & os.O_EXCL
    assert temp_opens[0][2] == 0o600
    assert fsynced_kinds == ["file", "dir"]
    assert len(replacements) == 1
    assert replacements[0][1] == os.fspath(root / "8" / f"{ref.object_id}.enc")
    assert not list(root.rglob("*.tmp"))


@pytest.mark.asyncio
async def test_store_enforces_private_directory_and_ciphertext_permissions(store, root):
    ref = await store.put_email(8, "mail-mode", {"body": "secret", "attachments": []})

    assert stat.S_IMODE(root.stat().st_mode) == 0o700
    assert stat.S_IMODE((root / "8").stat().st_mode) == 0o700
    assert stat.S_IMODE((root / "8" / f"{ref.object_id}.enc").stat().st_mode) == 0o600


@pytest.mark.asyncio
async def test_pre_replace_flush_failure_removes_its_temp(
    store,
    root,
    monkeypatch,
):
    from src.storage import ContentStoreWriteError, encrypted_files

    original_fdopen = os.fdopen

    class FlushFailingHandle:
        def __init__(self, handle):
            self._handle = handle

        def __enter__(self):
            self._handle.__enter__()
            return self

        def __exit__(self, *args):
            return self._handle.__exit__(*args)

        def __getattr__(self, name):
            return getattr(self._handle, name)

        def flush(self):
            raise OSError("sensitive-flush-error")

    def failing_fdopen(*args, **kwargs):
        return FlushFailingHandle(original_fdopen(*args, **kwargs))

    monkeypatch.setattr(encrypted_files.os, "fdopen", failing_fdopen)
    with pytest.raises(ContentStoreWriteError, match="content_write_failed") as caught:
        await store.put_email(8, "mail-flush-fail", {"body": "secret"})

    assert "sensitive-flush-error" not in str(caught.value)
    assert not list(root.rglob("*.tmp"))
    assert not list(root.rglob("*.enc"))


@pytest.mark.asyncio
async def test_pre_replace_file_fsync_failure_removes_only_its_temp(
    store,
    root,
    monkeypatch,
):
    from src.storage import ContentStoreWriteError, encrypted_files

    unrelated = root.parent / "unrelated.tmp"
    unrelated.write_bytes(b"keep")

    def fail_fsync(_fd):
        raise OSError("sensitive-os-error")

    monkeypatch.setattr(encrypted_files.os, "fsync", fail_fsync)
    with pytest.raises(ContentStoreWriteError, match="content_write_failed") as caught:
        await store.put_email(8, "mail-fsync-fail", {"body": "secret", "attachments": []})

    assert "sensitive-os-error" not in str(caught.value)
    assert unrelated.read_bytes() == b"keep"
    assert not list(root.rglob("*.tmp"))
    assert not list(root.rglob("*.enc"))


@pytest.mark.asyncio
async def test_pre_replace_failure_cleans_temp_without_touching_siblings(
    store,
    root,
    monkeypatch,
):
    from src.storage import ContentStoreWriteError, encrypted_files

    root.mkdir(mode=0o700)
    account_dir = root / "8"
    account_dir.mkdir(mode=0o700)
    sibling = account_dir / ".unrelated.tmp"
    sibling.write_bytes(b"keep")

    def fail_replace(_source, _target):
        raise OSError("sensitive-replace-error")

    monkeypatch.setattr(encrypted_files.os, "replace", fail_replace)
    with pytest.raises(ContentStoreWriteError, match="content_write_failed"):
        await store.put_email(8, "mail-replace-fail", {"body": "secret", "attachments": []})

    assert sibling.read_bytes() == b"keep"
    assert list(account_dir.glob("*.tmp")) == [sibling]
    assert not list(account_dir.glob("*.enc"))


@pytest.mark.asyncio
async def test_parent_fsync_failure_leaves_only_a_complete_authenticated_final(
    store,
    root,
    monkeypatch,
):
    from src.storage import ContentRef, ContentStoreWriteError, encrypted_files
    from src.storage.content_store import serialize_email_envelope

    original_fsync = os.fsync
    calls = 0

    def fail_second_fsync(fd):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("sensitive-parent-error")
        return original_fsync(fd)

    monkeypatch.setattr(encrypted_files.os, "fsync", fail_second_fsync)
    email = {"body": "complete", "attachments": []}
    with pytest.raises(ContentStoreWriteError, match="content_write_failed"):
        await store.put_email(8, "mail-parent-fail", email)

    files = list((root / "8").glob("*.enc"))
    assert len(files) == 1
    assert not list((root / "8").glob("*.tmp"))
    object_id = files[0].stem
    envelope = serialize_email_envelope("mail-parent-fail", email)
    ref = ContentRef(8, object_id, "v1", hashlib.sha256(envelope).hexdigest())
    assert await store.load_email(ref) == email


@pytest.mark.asyncio
async def test_delete_is_idempotent_and_fsyncs_parent(store, root, monkeypatch):
    from src.storage import ContentStoreNotFoundError, encrypted_files

    ref = await store.put_email(8, "mail-delete", {"body": "secret", "attachments": []})
    original_fsync = os.fsync
    fsynced_kinds = []

    def tracked_fsync(fd):
        mode = os.fstat(fd).st_mode
        fsynced_kinds.append("dir" if stat.S_ISDIR(mode) else "file")
        return original_fsync(fd)

    monkeypatch.setattr(encrypted_files.os, "fsync", tracked_fsync)
    await store.delete(ref)
    await store.delete(ref)

    assert fsynced_kinds == ["dir"]
    with pytest.raises(ContentStoreNotFoundError, match="content_not_found"):
        await store.load_email(ref)


@pytest.mark.asyncio
async def test_concurrent_puts_allocate_distinct_objects_with_atomic_reads(store):
    refs = await __import__("asyncio").gather(
        *(
            store.put_email(
                8,
                f"mail-{index}",
                {"body": f"body-{index}", "attachments": []},
            )
            for index in range(24)
        )
    )

    assert len({ref.object_id for ref in refs}) == 24
    loaded = await __import__("asyncio").gather(
        *(store.load_email(ref) for ref in refs)
    )
    assert {item["body"] for item in loaded} == {f"body-{index}" for index in range(24)}


async def _assert_heartbeat_runs_while(operation):
    import asyncio

    ticks = 0
    stopped = asyncio.Event()

    async def heartbeat():
        nonlocal ticks
        while not stopped.is_set():
            ticks += 1
            await asyncio.sleep(0.005)

    heartbeat_task = asyncio.create_task(heartbeat())
    await asyncio.sleep(0)
    operation_task = asyncio.create_task(operation())
    try:
        await asyncio.sleep(0.04)
        assert ticks >= 4
        await operation_task
    finally:
        stopped.set()
        await heartbeat_task


@pytest.mark.asyncio
async def test_put_offloads_serialization_crypto_and_filesystem(store, monkeypatch):
    original = store._put_email_sync

    def slow_put(*args):
        time.sleep(0.12)
        return original(*args)

    monkeypatch.setattr(store, "_put_email_sync", slow_put)
    await _assert_heartbeat_runs_while(
        lambda: store.put_email(8, "mail-nonblocking-put", {"body": "secret", "attachments": []})
    )


@pytest.mark.asyncio
async def test_load_offloads_crypto_parsing_and_filesystem(store, monkeypatch):
    ref = await store.put_email(8, "mail-nonblocking-load", {"body": "secret", "attachments": []})
    original = store._load_email_sync

    def slow_load(*args):
        time.sleep(0.12)
        return original(*args)

    monkeypatch.setattr(store, "_load_email_sync", slow_load)
    await _assert_heartbeat_runs_while(lambda: store.load_email(ref))


@pytest.mark.asyncio
async def test_delete_offloads_filesystem_and_directory_fsync(store, monkeypatch):
    ref = await store.put_email(8, "mail-nonblocking-delete", {"body": "secret", "attachments": []})
    original = store._delete_sync

    def slow_delete(*args):
        time.sleep(0.12)
        return original(*args)

    monkeypatch.setattr(store, "_delete_sync", slow_delete)
    await _assert_heartbeat_runs_while(lambda: store.delete(ref))
