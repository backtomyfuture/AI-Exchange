"""Unit coverage for the deployed PostgreSQL column-type contract."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from src.db import access_contract, schema_contract


def test_phase2_manifest_tracks_ignored_policy_revision() -> None:
    assert access_contract.PHASE2_DATABASE_REVISION == "20260713_0004"
    assert (
        access_contract.PHASE2_CHECK_CONSTRAINT_SHA256[
            ("event_inbox", "ck_event_inbox_processing_policy")
        ]
        == "d8fa97e98d89b2275a29c6899ce83136be195423cfdb907070a06074a4d7ab7c"
    )


class _ContractCursor:
    def __init__(self, result_batches):
        self.result_batches = list(result_batches)
        self.statements = []

    async def execute(self, statement, params=None):
        self.statements.append((statement, params))

    async def fetchall(self):
        return self.result_batches.pop(0)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return False


class _ContractConnection:
    def __init__(self, rows, *, structure_batches=None, check_manifest=None):
        phase2_present = any(row[0] in schema_contract.PHASE2_RELATIONS for row in rows)
        if structure_batches is None:
            if phase2_present:
                checks = [
                    (relation, name, digest, True, False)
                    for (relation, name), digest in (
                        (
                            check_manifest
                            or schema_contract.PHASE2_CHECK_CONSTRAINT_SHA256
                        ).items()
                    )
                ]
                unique = [
                    (
                        spec.relation,
                        spec.name,
                        spec.name,
                        spec.constraint_type,
                        list(spec.columns),
                        list(spec.index_options),
                        None,
                        spec.nulls_not_distinct,
                        spec.deferrable,
                        spec.initially_deferred,
                        spec.validated,
                        spec.index_valid,
                        spec.index_ready,
                        spec.access_method,
                        spec.has_no_included_columns,
                        spec.has_only_plain_columns,
                        spec.uses_default_operator_classes,
                        spec.uses_default_collations,
                    )
                    for spec in schema_contract.PHASE2_UNIQUE_CONSTRAINTS
                ]
                indexes = [
                    (
                        spec.relation,
                        spec.name,
                        spec.unique,
                        True,
                        True,
                        True,
                        True,
                        True,
                        "btree",
                        list(spec.columns),
                        list(spec.options),
                        spec.predicate_sha256,
                    )
                    for spec in schema_contract.PHASE2_INDEX_SPECS
                ]
            else:
                checks, unique, indexes = [], [], []
            structure_batches = (checks, unique, indexes)
        relation_names = {row[0] for row in rows}
        relation_kinds = [
            (
                name,
                relation_kind,
                "p",
                False,
                False,
                True,
                "heap" if relation_kind in {"r", "p"} else None,
            )
            for name, relation_kind in {
                **schema_contract._BASE_RELATION_KINDS,
                **(schema_contract._PHASE2_RELATION_KINDS if phase2_present else {}),
            }.items()
            if name in relation_names
        ]
        self.cursor_obj = _ContractCursor([relation_kinds, rows, *structure_batches])

    def cursor(self):
        return self.cursor_obj

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return False


def _deployed_rows(*, include_phase2: bool = True):
    return [
        (
            relation,
            column,
            "pg_catalog",
            type_name,
            (relation, column) in schema_contract._PHASE2_NULLABLE_COLUMNS,
            (relation, column) in schema_contract._PHASE2_DEFAULTED_COLUMNS,
            68 if type_name == "bpchar" else -1,
            schema_contract.PHASE2_DEFAULT_EXPRESSIONS.get((relation, column)),
            "",
            "",
            True,
            True,
            "",
        )
        for (
            relation,
            column,
        ), type_name in schema_contract._EXPECTED_COLUMN_TYPES.items()
        if include_phase2 or relation not in schema_contract.PHASE2_RELATIONS
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
async def test_schema_contract_accepts_complete_0002_catalog_types():
    connection = _ContractConnection(_deployed_rows(include_phase2=False))

    async def connect(*_args, **_kwargs):
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
            expected_revision="20260710_0002",
        )


@pytest.mark.asyncio
async def test_schema_contract_accepts_complete_0004_catalog_types():
    check_manifest = dict(schema_contract.PHASE2_CHECK_CONSTRAINT_SHA256)
    check_manifest[("event_inbox", "ck_event_inbox_processing_policy")] = (
        "d8fa97e98d89b2275a29c6899ce83136be195423cfdb907070a06074a4d7ab7c"
    )
    connection = _ContractConnection(
        _deployed_rows(),
        check_manifest=check_manifest,
    )

    async def connect(*_args, **_kwargs):
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
            expected_revision="20260713_0004",
        )


@pytest.mark.asyncio
async def test_schema_contract_keeps_exact_0003_policy_digest_compatibility():
    check_manifest = dict(schema_contract.PHASE2_CHECK_CONSTRAINT_SHA256)
    check_manifest[("event_inbox", "ck_event_inbox_processing_policy")] = (
        "f2c35a7d5a10689cc78f15a3d83cf656c89dc26578f2390519a7679012f1d9bb"
    )
    connection = _ContractConnection(
        _deployed_rows(),
        check_manifest=check_manifest,
    )

    async def connect(*_args, **_kwargs):
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
            expected_revision="20260710_0003",
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("expected_revision", "deployed_policy_digest"),
    [
        (
            "20260713_0004",
            "f2c35a7d5a10689cc78f15a3d83cf656c89dc26578f2390519a7679012f1d9bb",
        ),
        (
            "20260710_0003",
            "d8fa97e98d89b2275a29c6899ce83136be195423cfdb907070a06074a4d7ab7c",
        ),
    ],
    ids=("0003-digest-as-0004", "0004-digest-as-0003"),
)
async def test_schema_contract_rejects_cross_revision_policy_digest(
    expected_revision: str,
    deployed_policy_digest: str,
) -> None:
    check_manifest = dict(schema_contract.PHASE2_CHECK_CONSTRAINT_SHA256)
    check_manifest[("event_inbox", "ck_event_inbox_processing_policy")] = (
        deployed_policy_digest
    )
    connection = _ContractConnection(
        _deployed_rows(),
        check_manifest=check_manifest,
    )

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
            "postgresql://runtime/private",
            target_schema="public",
            require_complete=True,
            expected_revision=expected_revision,
        )


@pytest.mark.asyncio
async def test_schema_contract_rejects_0003_with_all_phase2_relations_missing():
    connection = _ContractConnection(_deployed_rows(include_phase2=False))

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
            "postgresql://runtime/private",
            target_schema="public",
            require_complete=True,
            expected_revision="20260710_0003",
        )


@pytest.mark.asyncio
async def test_schema_contract_rejects_target_domain_with_builtin_name():
    connection = _ContractConnection(
        [
            (
                "emails_log",
                "id",
                "public",
                "text",
                False,
                False,
                -1,
                None,
                "",
                "",
                True,
                True,
                "",
            )
        ]
    )

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
    connection = _ContractConnection(
        [
            (
                "emails_log",
                "id",
                "pg_catalog",
                "text",
                False,
                False,
                -1,
                None,
                "",
                "",
                True,
                True,
                "",
            )
        ]
    )

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
async def test_schema_contract_rejects_unexpected_column_after_bootstrap():
    connection = _ContractConnection(
        [
            *_deployed_rows(),
            (
                "event_inbox",
                "hidden_state",
                "pg_catalog",
                "text",
                True,
                False,
                -1,
                None,
                "",
                "",
                True,
                True,
                "",
            ),
        ]
    )

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
@pytest.mark.parametrize(
    ("column", "metadata_index", "invalid_value"),
    [
        (("event_inbox", "account_id"), 4, True),
        (("event_inbox", "attempts"), 5, False),
        (("event_inbox", "dedupe_key"), 6, -1),
        (("event_inbox", "attempts"), 7, "1"),
        (("event_inbox", "account_id"), 8, "d"),
        (("event_inbox", "account_id"), 9, "s"),
        (("emails", "external_email_id"), 10, False),
        (("event_inbox", "payload"), 11, False),
        (("event_inbox", "payload"), 12, "p"),
    ],
)
async def test_schema_contract_rejects_phase2_column_metadata_drift(
    column,
    metadata_index,
    invalid_value,
):
    rows = _deployed_rows()
    for index, row in enumerate(rows):
        if row[:2] == column:
            mutated = list(row)
            mutated[metadata_index] = invalid_value
            rows[index] = tuple(mutated)
            break
    connection = _ContractConnection(rows)

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
@pytest.mark.parametrize(
    ("metadata_index", "invalid_value"),
    [
        (2, "u"),
        (3, True),
        (4, True),
        (5, False),
        (6, "unexpected"),
    ],
    ids=(
        "unlogged",
        "row-security",
        "forced-row-security",
        "policy",
        "non-heap",
    ),
)
async def test_schema_contract_rejects_relation_metadata_drift(
    metadata_index,
    invalid_value,
):
    connection = _ContractConnection(_deployed_rows())
    relation_rows = connection.cursor_obj.result_batches[0]
    row_index = next(
        index for index, row in enumerate(relation_rows) if row[0] == "event_inbox"
    )
    mutated = list(relation_rows[row_index])
    mutated[metadata_index] = invalid_value
    relation_rows[row_index] = tuple(mutated)

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
            expected_revision="20260713_0004",
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("structure_batch_index", [2, 3, 4])
async def test_schema_contract_rejects_phase2_constraint_or_index_drift(
    structure_batch_index,
):
    connection = _ContractConnection(_deployed_rows())
    connection.cursor_obj.result_batches[structure_batch_index] = (
        connection.cursor_obj.result_batches[structure_batch_index][:-1]
    )

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
            expected_revision="20260713_0004",
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("metadata_index", "invalid_value"),
    [
        (5, [1]),
        (6, "unexpected-predicate-digest"),
        (7, True),
        (8, True),
        (9, True),
        (10, False),
        (11, False),
        (12, False),
        (13, "hash"),
        (14, False),
        (15, False),
        (16, False),
        (17, False),
    ],
    ids=(
        "index-options",
        "predicate",
        "nulls-not-distinct",
        "deferrable",
        "initially-deferred",
        "constraint-unvalidated",
        "index-invalid",
        "index-not-ready",
        "non-btree",
        "included-column",
        "expression-key",
        "nondefault-opclass",
        "nondefault-collation",
    ),
)
async def test_schema_contract_rejects_unique_backing_index_metadata_drift(
    metadata_index,
    invalid_value,
):
    connection = _ContractConnection(_deployed_rows())
    unique_rows = connection.cursor_obj.result_batches[3]
    mutated = list(unique_rows[0])
    mutated[metadata_index] = invalid_value
    unique_rows[0] = tuple(mutated)

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
            expected_revision="20260713_0004",
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
