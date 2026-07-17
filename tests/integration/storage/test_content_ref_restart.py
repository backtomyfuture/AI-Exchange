from __future__ import annotations

import base64
import json
from dataclasses import asdict

import pytest


@pytest.mark.asyncio
async def test_content_ref_survives_new_store_instance(tmp_path):
    from src.storage import ContentRef, EncryptedFileContentStore

    root = tmp_path / "content"
    key = base64.b64encode(bytes(range(32))).decode("ascii")
    first = EncryptedFileContentStore(root=root, key=key, key_version="v1")
    ref = await first.put_email(
        8,
        "mail-restart",
        {"body": "persisted", "attachments": []},
    )

    serialized_ref = json.dumps(asdict(ref), sort_keys=True)
    del first
    restored_ref = ContentRef(**json.loads(serialized_ref))
    second = EncryptedFileContentStore(root=root, key=key, key_version="v1")

    assert (await second.load_email(restored_ref))["body"] == "persisted"
