"""Unit coverage for the deployed PostgreSQL column-type contract."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from src.db import schema_contract


class _ContractCursor:
    def __init__(self, rows):
        self.rows = rows
        self.statements = []

    async def execute(self, statement, params=None):
        self.statements.append((statement, params))

    async def fetchall(self):
        return self.rows

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return False


class _ContractConnection:
    def __init__(self, rows):
        self.cursor_obj = _ContractCursor(rows)

    def cursor(self):
        return self.cursor_obj

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return False


def _deployed_rows():
    return [
        (relation, column, "pg_catalog", type_name)
        for (
            relation,
            column,
        ), type_name in schema_contract._EXPECTED_COLUMN_TYPES.items()
    ]


@pytest.mark.asyncio
async def test_schema_contract_accepts_complete_catalog_types():
    connection = _ContractConnection(_deployed_rows())

    async def connect(*_args, **kwargs):
        assert kwargs["autocommit"] is True
        return connection

    with patch.object(
        schema_contract.psycopg.AsyncConnection,
        "connect",
        side_effect=connect,
    ):
        await schema_contract.require_database_schema_contract(
            "postgresql://runtime/private",
            target_schema="public",
            require_complete=True,
        )

    assert connection.cursor_obj.statements[0] == (
        "SET search_path TO pg_catalog",
        None,
    )


@pytest.mark.asyncio
async def test_schema_contract_rejects_target_domain_with_builtin_name():
    connection = _ContractConnection([("emails_log", "id", "public", "text")])

    async def connect(*_args, **_kwargs):
        return connection

    with (
        patch.object(
            schema_contract.psycopg.AsyncConnection,
            "connect",
            side_effect=connect,
        ),
        pytest.raises(
            schema_contract.DatabaseSchemaContractError,
            match="database_schema_contract_invalid",
        ),
    ):
        await schema_contract.require_database_schema_contract(
            "postgresql://migration/private",
            target_schema="public",
            require_complete=False,
        )


@pytest.mark.asyncio
async def test_schema_contract_requires_every_known_column_after_bootstrap():
    connection = _ContractConnection([("emails_log", "id", "pg_catalog", "text")])

    async def connect(*_args, **_kwargs):
        return connection

    with (
        patch.object(
            schema_contract.psycopg.AsyncConnection,
            "connect",
            side_effect=connect,
        ),
        pytest.raises(
            schema_contract.DatabaseSchemaContractError,
            match="database_schema_contract_invalid",
        ),
    ):
        await schema_contract.require_database_schema_contract(
            "postgresql://migration/private",
            target_schema="public",
            require_complete=True,
        )


@pytest.mark.asyncio
async def test_schema_contract_cuts_off_private_connection_errors():
    private_dsn = "postgresql://private-user:private-password@db/private"

    async def connect(*_args, **_kwargs):
        raise RuntimeError(f"connection failed: {private_dsn}")

    with (
        patch.object(
            schema_contract.psycopg.AsyncConnection,
            "connect",
            side_effect=connect,
        ),
        pytest.raises(schema_contract.DatabaseSchemaContractError) as caught,
    ):
        await schema_contract.require_database_schema_contract(
            private_dsn,
            target_schema="public",
            require_complete=True,
        )

    assert str(caught.value) == "database_schema_contract_invalid"
    assert private_dsn not in str(caught.value)
    assert caught.value.__cause__ is None
