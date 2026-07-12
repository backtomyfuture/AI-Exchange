from unittest.mock import AsyncMock

import psycopg
import pytest

from src.maintenance.checkpoint_repository import (
    _ELIGIBLE_THREAD_STATS_SQL,
    _LATEST_CHECKPOINT_SHAPE_SQL,
    CheckpointRepositoryError,
    _database_fingerprint,
    _read_database_metadata,
)


def test_candidate_limit_is_applied_before_checkpoint_lateral_aggregates():
    normalized = " ".join(_ELIGIBLE_THREAD_STATS_SQL.split()).upper()

    assert "WITH ELIGIBLE_EMAILS AS MATERIALIZED" in normalized
    assert normalized.index("LIMIT %S") < normalized.index(
        "CROSS JOIN LATERAL"
    )


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
