import inspect
from unittest.mock import AsyncMock

import psycopg
import pytest

from src.maintenance.checkpoint_repository import (
    CHECKPOINT_CLEANUP_COMPATIBLE_DATABASE_REVISIONS,
    CheckpointExecutionSession,
    _ELIGIBLE_THREAD_STATS_SQL,
    _LATEST_CHECKPOINT_SHAPE_SQL,
    CheckpointRepositoryError,
    _database_fingerprint,
    _inventory_digest,
    _read_database_metadata,
)


def test_cleanup_revision_allowlist_is_fixed_and_independent_from_runtime_bridge():
    from src.maintenance import checkpoint_repository

    assert CHECKPOINT_CLEANUP_COMPATIBLE_DATABASE_REVISIONS == frozenset(
        {"20260710_0002", "20260710_0003"}
    )
    assert "RUNTIME_COMPATIBLE_DATABASE_REVISIONS" not in inspect.getsource(
        checkpoint_repository
    )


def test_candidate_limit_is_applied_before_checkpoint_lateral_aggregates():
    normalized = " ".join(_ELIGIBLE_THREAD_STATS_SQL.split()).upper()

    assert "WITH ELIGIBLE_EMAILS AS MATERIALIZED" in normalized
    assert normalized.index("LIMIT %S") < normalized.index("CROSS JOIN LATERAL")


def test_delete_uses_quiesced_plain_email_select_without_write_privilege_locks():
    source = " ".join(
        inspect.getsource(CheckpointExecutionSession.delete_candidate).split()
    ).upper()

    assert "SELECT ID FROM EMAILS_LOG WHERE ID = %S" in source
    assert "EMAILS_LOG WHERE ID = %S FOR UPDATE" not in source
    assert "LOCK TABLE EMAILS_LOG" not in source
    assert "LOCK TABLE CHECKPOINTS, CHECKPOINT_BLOBS, CHECKPOINT_WRITES" in source
    assert "IN SHARE ROW EXCLUSIVE MODE" in source


def test_shape_query_projects_only_required_versions_and_inline_scalars():
    normalized = " ".join(_LATEST_CHECKPOINT_SHAPE_SQL.split()).upper()

    assert "CHECKPOINT -> 'CHANNEL_VALUES'," not in normalized
    assert "CHECKPOINT -> 'CHANNEL_VERSIONS'," not in normalized
    for channel in (
        "EMAIL_ID",
        "CONTENT_REF",
        "ATTACHMENT_TOKENS",
        "PDF_TOKEN",
    ):
        assert f"CHANNEL_VERSIONS,{channel.lower()}".upper() in normalized


def test_inventory_digest_uses_authorized_row_content_not_system_columns():
    source = " ".join(inspect.getsource(_inventory_digest).split()).upper()

    assert "XMIN" not in source
    assert source.count("PG_CATALOG.SHA256") == 4
    assert source.count("PG_CATALOG.ENCODE") == 4
    for domain in (
        "CHECKPOINT:V1",
        "METADATA:V1",
        "CHECKPOINT_BLOB:V1",
        "CHECKPOINT_WRITE_BLOB:V1",
    ):
        assert domain in source
    assert source.count("WHEN BLOB IS NULL") == 2
    assert source.count("PG_CATALOG.CONVERT_TO('NULL', 'UTF8')") == 2
    assert "COALESCE(PARENT_CHECKPOINT_ID, '')" not in source
    assert "COALESCE(TYPE, '')" not in source
    assert source.count("ARRAY['NULL']::PG_CATALOG.TEXT[]") == 3
    assert source.count("ARRAY['TEXT',") == 3
    assert "CHECKPOINT::TEXT, METADATA::TEXT FROM CHECKPOINTS" not in source
    assert ", BLOB FROM CHECKPOINT_BLOBS" not in source
    assert ", BLOB FROM CHECKPOINT_WRITES" not in source
    assert "CONTENT_SHA256" not in source


def test_database_fingerprint_binds_postgres_cluster_system_identifier():
    common_identity = {
        "database": "mail",
        "schema": "public",
        "server_address": "local",
        "server_port": 0,
        "server_version": "150016",
    }

    first = _database_fingerprint(
        **common_identity,
        system_identifier="7500000000000000001",
    )
    second = _database_fingerprint(
        **common_identity,
        system_identifier="7500000000000000002",
    )

    assert first != second
    assert len(first) == len(second) == 64


@pytest.mark.asyncio
async def test_database_identity_permission_failure_is_fixed_and_cause_free():
    connection = AsyncMock()
    connection.execute.side_effect = psycopg.errors.InsufficientPrivilege()

    with pytest.raises(CheckpointRepositoryError) as error:
        await _read_database_metadata(connection)

    assert error.value.code == "cleanup_database_identity_unavailable"
    assert str(error.value) == "cleanup_database_identity_unavailable"
    assert error.value.__cause__ is None
    assert error.value.__suppress_context__ is True
