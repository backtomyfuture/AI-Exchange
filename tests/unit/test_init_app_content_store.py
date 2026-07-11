from __future__ import annotations

import base64
import logging
from unittest.mock import MagicMock, patch

import pytest
from pydantic import SecretStr


def _settings(tmp_path, *, key: str):
    settings = MagicMock()
    settings.CONTENT_STORE_ROOT = str(tmp_path / "content")
    settings.CONTENT_STORE_KEY = SecretStr(key)
    settings.CONTENT_STORE_KEY_VERSION = "v1"
    settings.database_url = "postgresql://user:password@localhost/email_agent"
    return settings


def _external_component_patches():
    return (
        patch("src.init_app.ExchangeClient"),
        patch("src.init_app.EmailProcessor"),
        patch("src.init_app.AsyncDatabaseManager"),
        patch("src.init_app.AsyncConnectionPool"),
    )


def test_get_app_context_wires_one_shared_content_store(tmp_path):
    from src import init_app
    from src.storage import EncryptedFileContentStore

    key = base64.b64encode(bytes(range(32))).decode("ascii")
    settings = _settings(tmp_path, key=key)
    context = init_app.AppContext()
    exchange_patch, processor_patch, db_patch, pool_patch = (
        _external_component_patches()
    )

    with (
        patch.object(init_app, "get_settings", return_value=settings),
        patch.object(init_app, "app_context", context),
        exchange_patch as exchange_client,
        processor_patch as email_processor,
        db_patch as db_manager,
        pool_patch as connection_pool,
    ):
        first = init_app.get_app_context()
        second = init_app.get_app_context()

    assert first is second is context
    assert isinstance(context.content_store, EncryptedFileContentStore)
    assert first.content_store is second.content_store
    exchange_client.assert_called_once_with(settings)
    email_processor.assert_called_once_with()
    db_manager.assert_called_once_with(settings)
    connection_pool.assert_called_once()


def test_invalid_content_store_config_fails_before_external_clients_and_hides_key(
    tmp_path,
    caplog,
):
    from src import init_app
    from src.storage import ContentStoreConfigurationError

    secret_marker = "not-base64-sensitive-key-material"
    settings = _settings(tmp_path, key=secret_marker)
    context = init_app.AppContext()
    exchange_patch, processor_patch, db_patch, pool_patch = (
        _external_component_patches()
    )
    caplog.set_level(logging.DEBUG)

    with (
        patch.object(init_app, "get_settings", return_value=settings),
        exchange_patch as exchange_client,
        processor_patch as email_processor,
        db_patch as db_manager,
        pool_patch as connection_pool,
        pytest.raises(
            ContentStoreConfigurationError,
            match="invalid_content_store_key",
        ),
    ):
        context.initialize()

    exchange_client.assert_not_called()
    email_processor.assert_not_called()
    db_manager.assert_not_called()
    connection_pool.assert_not_called()
    assert context.content_store is None
    assert secret_marker not in caplog.text
