"""Catalog checks for the one supported empty-database baseline."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from src.db import schema_contract


class _Cursor:
    def __init__(self, responses: list[list[tuple[object, ...]]]) -> None:
        self._responses = responses
        self.statements: list[object] = []

    async def execute(self, statement: object, _params: tuple[object, ...] = ()) -> None:
        self.statements.append(statement)

    async def fetchall(self) -> list[tuple[object, ...]]:
        return self._responses.pop(0)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return False


class _Connection:
    def __init__(self, cursor: _Cursor) -> None:
        self.cursor_obj = cursor

    def cursor(self) -> _Cursor:
        return self.cursor_obj

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return False


def _complete_catalog(*, retired_relation: str | None = None, retired_routine: str | None = None, wrong_column: tuple[str, str] | None = None) -> list[list[tuple[object, ...]]]:
    relation_kinds = {
        **schema_contract._BUSINESS_RELATIONS,
        **schema_contract._CHECKPOINT_RELATIONS,
    }
    if retired_relation is not None:
        relation_kinds[retired_relation] = "r"
    columns: list[tuple[object, ...]] = []
    for (relation, column), type_name in schema_contract._REQUIRED_COLUMN_TYPES.items():
        actual_type = "text" if (relation, column) == wrong_column else type_name
        columns.append((relation, column, "pg_catalog", actual_type))
    routines = [(name,) for name in schema_contract._REQUIRED_ROUTINES]
    if retired_routine is not None:
        routines.append((retired_routine,))
    return [
        list(relation_kinds.items()),
        columns,
        routines,
        [(schema_contract.GREENFIELD_DATABASE_REVISION,)],
    ]


async def _require_with_catalog(
    responses: list[list[tuple[object, ...]]],
    **kwargs: object,
) -> None:
    connection = _Connection(_Cursor(responses))
    with patch(
        "src.db.schema_contract.psycopg.AsyncConnection.connect",
        new=AsyncMock(return_value=connection),
    ):
        await schema_contract.require_database_schema_contract(
            "postgresql://test/test",
            target_schema="public",
            **kwargs,
        )


def test_contract_names_the_one_greenfield_revision_and_current_catalog() -> None:
    assert schema_contract.GREENFIELD_DATABASE_REVISION == "20260808_0001"
    assert "sync_cold_start_plans" in schema_contract._RETIRED_RELATIONS
    assert "greenfield_insert_webhook_event" in schema_contract._RETIRED_ROUTINES
    assert "greenfield_commit_sync_page" in schema_contract._REQUIRED_ROUTINES


@pytest.mark.asyncio
async def test_contract_allows_an_empty_database_before_bootstrap() -> None:
    await _require_with_catalog(
        [[], [], []],
        require_complete=False,
        require_business_complete=False,
    )


@pytest.mark.asyncio
async def test_contract_rejects_catalog_shadows_before_greenfield_bootstrap() -> None:
    with pytest.raises(schema_contract.DatabaseSchemaContractError):
        await _require_with_catalog(
            [
                [("pg_class", "v")],
                [],
                [("current_schema",)],
            ],
            require_complete=False,
            require_business_complete=False,
        )


@pytest.mark.asyncio
async def test_contract_accepts_the_complete_baseline_catalog() -> None:
    await _require_with_catalog(
        _complete_catalog(),
        require_complete=True,
        expected_revision=schema_contract.GREENFIELD_DATABASE_REVISION,
    )


@pytest.mark.asyncio
async def test_contract_accepts_business_baseline_before_checkpoint_setup() -> None:
    catalog = _complete_catalog()
    catalog[0] = [
        row for row in catalog[0] if row[0] not in schema_contract._CHECKPOINT_RELATIONS
    ]
    catalog[1] = [
        row for row in catalog[1] if row[0] not in schema_contract._CHECKPOINT_RELATIONS
    ]

    await _require_with_catalog(
        catalog,
        require_complete=False,
        require_business_complete=True,
        expected_revision=schema_contract.GREENFIELD_DATABASE_REVISION,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("expected_revision", ["obsolete", "another-head"])
async def test_contract_rejects_historical_revision_requests_before_connecting(
    expected_revision: str,
) -> None:
    connect = AsyncMock()
    with (
        patch(
            "src.db.schema_contract.psycopg.AsyncConnection.connect",
            new=connect,
        ),
        pytest.raises(schema_contract.DatabaseSchemaContractError),
    ):
        await schema_contract.require_database_schema_contract(
            "postgresql://test/test",
            target_schema="public",
            require_complete=True,
            expected_revision=expected_revision,
        )

    connect.assert_not_awaited()


@pytest.mark.asyncio
async def test_contract_rejects_a_retired_relation() -> None:
    with pytest.raises(schema_contract.DatabaseSchemaContractError):
        await _require_with_catalog(
            _complete_catalog(retired_relation="sync_cold_start_plans"),
            require_complete=True,
            expected_revision=schema_contract.GREENFIELD_DATABASE_REVISION,
        )


@pytest.mark.asyncio
async def test_contract_rejects_a_required_column_type_drift() -> None:
    with pytest.raises(schema_contract.DatabaseSchemaContractError):
        await _require_with_catalog(
            _complete_catalog(wrong_column=("event_inbox", "payload")),
            require_complete=True,
            expected_revision=schema_contract.GREENFIELD_DATABASE_REVISION,
        )


@pytest.mark.asyncio
async def test_contract_rejects_checkpoint_migration_type_drift() -> None:
    catalog = _complete_catalog()
    catalog[1].append(
        ("checkpoint_migrations", "v", "public", "checkpoint_version")
    )

    with pytest.raises(schema_contract.DatabaseSchemaContractError):
        await _require_with_catalog(
            catalog,
            require_complete=True,
            expected_revision=schema_contract.GREENFIELD_DATABASE_REVISION,
        )


@pytest.mark.asyncio
async def test_contract_rejects_a_retired_routine() -> None:
    with pytest.raises(schema_contract.DatabaseSchemaContractError):
        await _require_with_catalog(
            _complete_catalog(retired_routine="greenfield_insert_webhook_event"),
            require_complete=True,
            expected_revision=schema_contract.GREENFIELD_DATABASE_REVISION,
        )
