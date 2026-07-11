from __future__ import annotations

import base64

import pytest


@pytest.fixture
def root(tmp_path):
    return tmp_path / "content"


@pytest.fixture
def valid_key() -> str:
    return base64.b64encode(bytes(range(32))).decode("ascii")


@pytest.fixture
def store(root, valid_key):
    from src.storage import EncryptedFileContentStore

    return EncryptedFileContentStore(root=root, key=valid_key, key_version="v1")


@pytest.fixture
def store_factory(root, valid_key):
    from src.storage import EncryptedFileContentStore

    def create_store():
        return EncryptedFileContentStore(root=root, key=valid_key, key_version="v1")

    return create_store
