"""PostgreSQL boundary for guarded LangGraph checkpoint cleanup.

The repository deliberately does not decide whether destructive execution is
authorized.  It proves candidate eligibility, takes a session advisory lock,
and supplies the per-thread transaction used by the higher-level cleaner after
the plan, backup receipt and service-quiescence gates have succeeded.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Final, Literal

import ormsgpack
import psycopg
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

from src.db.maintenance_fence import (
    CHECKPOINT_MAINTENANCE_LOCK_KEY,
    DEFAULT_MAINTENANCE_EXCLUSIVE_SETTLE_SECONDS,
)
from src.maintenance.cleanup_models import (
    EXCLUSION_REASONS,
    MINIMUM_CLEANUP_AGE,
    TERMINAL_CHECKPOINT_STATUSES,
    CleanupCandidate,
    ExclusionBucket,
)


CHECKPOINT_CLEANUP_COMPATIBLE_DATABASE_REVISIONS: Final = frozenset({"20260808_0001"})
MAX_PLAN_THREADS: Final = 100
MAX_PHYSICAL_ROWS: Final = 500
MAX_ESTIMATED_LOGICAL_BYTES: Final = 64 * 1024 * 1024
MAX_INSPECTED_THREADS: Final = 1_000
MAX_CLEANUP_HANDLE_BLOB_BYTES: Final = 16 * 1024
DEFAULT_LOCK_TIMEOUT_MS: Final = 2_000
DEFAULT_STATEMENT_TIMEOUT_MS: Final = 15_000
DEFAULT_IDLE_TRANSACTION_TIMEOUT_MS: Final = 15_000

_TERMINAL_STATUS_SQL = "('sent', 'rejected', 'draft_saved')"
_SAFE_ERROR_CODES = frozenset(
    {
        "cleanup_repository_failed",
        "cleanup_scan_failed",
        "cleanup_revalidation_failed",
        "cleanup_delete_failed",
        "cleanup_delete_confirmation_failed",
        "cleanup_lock_unavailable",
        "cleanup_invalid_cutoff",
        "cleanup_cutoff_too_recent",
        "cleanup_invalid_limit",
        "cleanup_invalid_row_budget",
        "cleanup_invalid_byte_budget",
        "cleanup_schema_revision_mismatch",
        "cleanup_checkpoint_migrations_mismatch",
        "cleanup_database_identity_unavailable",
        "cleanup_plan_invalid",
        "cleanup_plan_database_mismatch",
        "cleanup_plan_timezone_mismatch",
        "cleanup_plan_schema_mismatch",
        "cleanup_plan_checkpoint_mismatch",
        "cleanup_plan_expired",
        "cleanup_candidate_not_in_plan",
    }
)


class CheckpointRepositoryError(RuntimeError):
    """A fail-closed repository error whose text never exposes DB details."""

    def __init__(self, code: str) -> None:
        safe_code = code if code in _SAFE_ERROR_CODES else "cleanup_repository_failed"
        self.code = safe_code
        super().__init__(safe_code)


@dataclass(frozen=True, slots=True)
class CheckpointScanSnapshot:
    """A repeatable-read scan result used to construct an immutable plan."""

    cutoff: datetime
    database_fingerprint: str
    database_timezone: str
    alembic_revision: str
    checkpoint_revision: int
    scanned_count: int
    candidates: tuple[CleanupCandidate, ...]
    excluded_buckets: tuple[ExclusionBucket, ...]

    @property
    def total_rows(self) -> int:
        return sum(candidate.total_rows for candidate in self.candidates)

    @property
    def estimated_logical_bytes(self) -> int:
        return sum(candidate.estimated_logical_bytes for candidate in self.candidates)


@dataclass(frozen=True, slots=True)
class CandidateDeleteResult:
    disposition: Literal["deleted", "stale"]
    checkpoint_rows: int
    checkpoint_blob_rows: int
    checkpoint_write_rows: int
    estimated_logical_bytes: int

    @property
    def deleted(self) -> bool:
        return self.disposition == "deleted"

    @property
    def stale(self) -> bool:
        return self.disposition == "stale"


@dataclass(frozen=True, slots=True)
class _DatabaseMetadata:
    database_fingerprint: str
    database_timezone: str
    alembic_revision: str
    checkpoint_revision: int


@dataclass(frozen=True, slots=True)
class _ThreadStats:
    thread_id: str
    status: str
    updated_at: datetime
    checkpoint_rows: int
    checkpoint_bytes: int
    checkpoint_blob_rows: int
    checkpoint_blob_bytes: int
    checkpoint_write_rows: int
    checkpoint_write_bytes: int
    has_non_default_namespace: bool

    @property
    def total_rows(self) -> int:
        return (
            self.checkpoint_rows
            + self.checkpoint_blob_rows
            + self.checkpoint_write_rows
        )

    @property
    def estimated_logical_bytes(self) -> int:
        return (
            self.checkpoint_bytes
            + self.checkpoint_blob_bytes
            + self.checkpoint_write_bytes
        )


@dataclass(frozen=True, slots=True)
class _InspectionResult:
    candidate: CleanupCandidate | None
    exclusion_reason: str | None


_BASE_COUNTS_SQL = f"""
    SELECT
        count(*)::bigint,
        count(*) FILTER (
            WHERE status IS NULL OR status NOT IN {_TERMINAL_STATUS_SQL}
        )::bigint,
        count(*) FILTER (
            WHERE status IN {_TERMINAL_STATUS_SQL} AND updated_at IS NULL
        )::bigint,
        count(*) FILTER (
            WHERE status IN {_TERMINAL_STATUS_SQL}
              AND updated_at IS NOT NULL
              AND NOT (
                  updated_at AT TIME ZONE current_setting('TimeZone') < %s
              )
        )::bigint,
        count(*) FILTER (
            WHERE status IN {_TERMINAL_STATUS_SQL}
              AND updated_at IS NOT NULL
              AND updated_at AT TIME ZONE current_setting('TimeZone') < %s
        )::bigint
    FROM emails_log
"""

_ELIGIBLE_THREAD_STATS_SQL = f"""
    WITH eligible_emails AS MATERIALIZED (
        SELECT id, status, updated_at
        FROM emails_log
        WHERE status IN {_TERMINAL_STATUS_SQL}
          AND updated_at IS NOT NULL
          AND updated_at AT TIME ZONE current_setting('TimeZone') < %s
        ORDER BY updated_at ASC, id ASC
        LIMIT %s
    )
    SELECT
        email.id,
        email.status,
        email.updated_at AT TIME ZONE current_setting('TimeZone'),
        checkpoint_stats.row_count,
        checkpoint_stats.logical_bytes,
        blob_stats.row_count,
        blob_stats.logical_bytes,
        write_stats.row_count,
        write_stats.logical_bytes,
        (
            checkpoint_stats.has_non_default_namespace
            OR blob_stats.has_non_default_namespace
            OR write_stats.has_non_default_namespace
        )
    FROM eligible_emails AS email
    CROSS JOIN LATERAL (
        SELECT
            count(*)::bigint AS row_count,
            coalesce(sum(
                octet_length(thread_id)
                + octet_length(checkpoint_ns)
                + octet_length(checkpoint_id)
                + coalesce(octet_length(parent_checkpoint_id), 0)
                + coalesce(octet_length(type), 0)
                + octet_length(checkpoint::text)
                + octet_length(metadata::text)
            ), 0)::bigint AS logical_bytes,
            coalesce(bool_or(checkpoint_ns <> ''), false)
                AS has_non_default_namespace
        FROM checkpoints AS checkpoint_row
        WHERE checkpoint_row.thread_id = email.id
    ) AS checkpoint_stats
    CROSS JOIN LATERAL (
        SELECT
            count(*)::bigint AS row_count,
            coalesce(sum(
                octet_length(thread_id)
                + octet_length(checkpoint_ns)
                + octet_length(channel)
                + octet_length(version)
                + octet_length(type)
                + coalesce(octet_length(blob), 0)
            ), 0)::bigint AS logical_bytes,
            coalesce(bool_or(checkpoint_ns <> ''), false)
                AS has_non_default_namespace
        FROM checkpoint_blobs AS blob_row
        WHERE blob_row.thread_id = email.id
    ) AS blob_stats
    CROSS JOIN LATERAL (
        SELECT
            count(*)::bigint AS row_count,
            coalesce(sum(
                octet_length(thread_id)
                + octet_length(checkpoint_ns)
                + octet_length(checkpoint_id)
                + octet_length(task_id)
                + 4
                + octet_length(channel)
                + coalesce(octet_length(type), 0)
                + octet_length(blob)
                + octet_length(task_path)
            ), 0)::bigint AS logical_bytes,
            coalesce(bool_or(checkpoint_ns <> ''), false)
                AS has_non_default_namespace
        FROM checkpoint_writes AS write_row
        WHERE write_row.thread_id = email.id
    ) AS write_stats
    ORDER BY email.updated_at ASC, email.id ASC
"""

_ONE_THREAD_STATS_SQL = """
    SELECT
        email.id,
        email.status,
        email.updated_at AT TIME ZONE current_setting('TimeZone'),
        checkpoint_stats.row_count,
        checkpoint_stats.logical_bytes,
        blob_stats.row_count,
        blob_stats.logical_bytes,
        write_stats.row_count,
        write_stats.logical_bytes,
        (
            checkpoint_stats.has_non_default_namespace
            OR blob_stats.has_non_default_namespace
            OR write_stats.has_non_default_namespace
        )
    FROM emails_log AS email
    CROSS JOIN LATERAL (
        SELECT
            count(*)::bigint AS row_count,
            coalesce(sum(
                octet_length(thread_id)
                + octet_length(checkpoint_ns)
                + octet_length(checkpoint_id)
                + coalesce(octet_length(parent_checkpoint_id), 0)
                + coalesce(octet_length(type), 0)
                + octet_length(checkpoint::text)
                + octet_length(metadata::text)
            ), 0)::bigint AS logical_bytes,
            coalesce(bool_or(checkpoint_ns <> ''), false)
                AS has_non_default_namespace
        FROM checkpoints AS checkpoint_row
        WHERE checkpoint_row.thread_id = email.id
    ) AS checkpoint_stats
    CROSS JOIN LATERAL (
        SELECT
            count(*)::bigint AS row_count,
            coalesce(sum(
                octet_length(thread_id)
                + octet_length(checkpoint_ns)
                + octet_length(channel)
                + octet_length(version)
                + octet_length(type)
                + coalesce(octet_length(blob), 0)
            ), 0)::bigint AS logical_bytes,
            coalesce(bool_or(checkpoint_ns <> ''), false)
                AS has_non_default_namespace
        FROM checkpoint_blobs AS blob_row
        WHERE blob_row.thread_id = email.id
    ) AS blob_stats
    CROSS JOIN LATERAL (
        SELECT
            count(*)::bigint AS row_count,
            coalesce(sum(
                octet_length(thread_id)
                + octet_length(checkpoint_ns)
                + octet_length(checkpoint_id)
                + octet_length(task_id)
                + 4
                + octet_length(channel)
                + coalesce(octet_length(type), 0)
                + octet_length(blob)
                + octet_length(task_path)
            ), 0)::bigint AS logical_bytes,
            coalesce(bool_or(checkpoint_ns <> ''), false)
                AS has_non_default_namespace
        FROM checkpoint_writes AS write_row
        WHERE write_row.thread_id = email.id
    ) AS write_stats
    WHERE email.id = %s
"""

_LATEST_CHECKPOINT_SHAPE_SQL = """
    SELECT
        checkpoint #>> '{channel_versions,email_id}',
        checkpoint #>> '{channel_versions,content_ref}',
        checkpoint #>> '{channel_versions,attachment_tokens}',
        checkpoint #>> '{channel_versions,pdf_token}',
        (checkpoint -> 'channel_values') ? 'email_id',
        jsonb_typeof(checkpoint #> '{channel_values,email_id}'),
        checkpoint #>> '{channel_values,email_id}',
        (checkpoint -> 'channel_values') ? 'content_ref',
        (checkpoint -> 'channel_values') ? 'attachment_tokens',
        (checkpoint -> 'channel_values') ? 'pdf_token',
        jsonb_typeof(checkpoint #> '{channel_values,pdf_token}')
    FROM checkpoints
    WHERE thread_id = %s AND checkpoint_ns = ''
    ORDER BY checkpoint_id DESC
    LIMIT 1
"""


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _canonical_digest(value: object) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _database_fingerprint(
    *,
    database: str,
    schema: str,
    server_address: str,
    server_port: int,
    server_version: str,
    system_identifier: str,
) -> str:
    """Hash a cluster-bound identity without exposing connection details."""

    return _canonical_digest(
        {
            "database": database,
            "schema": schema,
            "server_address": server_address,
            "server_port": server_port,
            "server_version": server_version,
            "system_identifier": system_identifier,
        }
    )


def _require_aware_utc(value: object) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise CheckpointRepositoryError("cleanup_invalid_cutoff")
    try:
        if value.utcoffset() is None:
            raise CheckpointRepositoryError("cleanup_invalid_cutoff")
    except (OverflowError, ValueError):
        raise CheckpointRepositoryError("cleanup_invalid_cutoff") from None
    return value.astimezone(UTC)


def _validate_scan_inputs(
    *,
    cutoff: object,
    now: datetime,
    limit: object,
    max_physical_rows: object,
    max_estimated_logical_bytes: object,
) -> datetime:
    normalized_cutoff = _require_aware_utc(cutoff)
    if now - normalized_cutoff < MINIMUM_CLEANUP_AGE:
        raise CheckpointRepositoryError("cleanup_cutoff_too_recent")
    if type(limit) is not int or not 1 <= limit <= MAX_PLAN_THREADS:
        raise CheckpointRepositoryError("cleanup_invalid_limit")
    if (
        type(max_physical_rows) is not int
        or not 1 <= max_physical_rows <= MAX_PHYSICAL_ROWS
    ):
        raise CheckpointRepositoryError("cleanup_invalid_row_budget")
    if (
        type(max_estimated_logical_bytes) is not int
        or not 1 <= max_estimated_logical_bytes <= MAX_ESTIMATED_LOGICAL_BYTES
    ):
        raise CheckpointRepositoryError("cleanup_invalid_byte_budget")
    return normalized_cutoff


def _stats_from_row(row: tuple[object, ...]) -> _ThreadStats:
    updated_at = row[2]
    if not isinstance(updated_at, datetime) or updated_at.tzinfo is None:
        raise ValueError("invalid timestamp")
    return _ThreadStats(
        thread_id=str(row[0]),
        status=str(row[1]),
        updated_at=updated_at.astimezone(UTC),
        checkpoint_rows=int(row[3]),
        checkpoint_bytes=int(row[4]),
        checkpoint_blob_rows=int(row[5]),
        checkpoint_blob_bytes=int(row[6]),
        checkpoint_write_rows=int(row[7]),
        checkpoint_write_bytes=int(row[8]),
        has_non_default_namespace=bool(row[9]),
    )


async def _read_database_metadata(
    conn: psycopg.AsyncConnection,
) -> _DatabaseMetadata:
    try:
        row = await (
            await conn.execute(
                """
                SELECT
                    current_database(),
                    current_schema(),
                    coalesce(inet_server_addr()::text, 'local'),
                    coalesce(inet_server_port(), 0),
                    current_setting('server_version_num'),
                    current_setting('TimeZone'),
                    (SELECT system_identifier::text FROM pg_control_system())
                """
            )
        ).fetchone()
    except psycopg.Error:
        raise CheckpointRepositoryError(
            "cleanup_database_identity_unavailable"
        ) from None

    if row is None or not isinstance(row[6], str) or not row[6]:
        raise CheckpointRepositoryError("cleanup_database_identity_unavailable")

    try:
        revision_rows = await (
            await conn.execute(
                "SELECT version_num FROM alembic_version ORDER BY version_num"
            )
        ).fetchall()
        migration_rows = await (
            await conn.execute("SELECT v FROM checkpoint_migrations ORDER BY v")
        ).fetchall()
    except psycopg.Error:
        raise CheckpointRepositoryError("cleanup_schema_revision_mismatch") from None

    actual_revisions = [str(item[0]) for item in revision_rows]
    if (
        row is None
        or len(actual_revisions) != 1
        or actual_revisions[0] not in CHECKPOINT_CLEANUP_COMPATIBLE_DATABASE_REVISIONS
    ):
        raise CheckpointRepositoryError("cleanup_schema_revision_mismatch")
    actual_revision = actual_revisions[0]

    expected_migrations = list(range(1, len(AsyncPostgresSaver.MIGRATIONS)))
    actual_migrations = [int(item[0]) for item in migration_rows]
    accepted_migration_histories = (
        expected_migrations,
        [0, *expected_migrations],
    )
    if actual_migrations not in accepted_migration_histories:
        raise CheckpointRepositoryError("cleanup_checkpoint_migrations_mismatch")

    fingerprint = _database_fingerprint(
        database=str(row[0]),
        schema=str(row[1]),
        server_address=str(row[2]),
        server_port=int(row[3]),
        server_version=str(row[4]),
        system_identifier=row[6],
    )
    return _DatabaseMetadata(
        database_fingerprint=fingerprint,
        database_timezone=str(row[5]),
        alembic_revision=actual_revision,
        checkpoint_revision=expected_migrations[-1],
    )


async def _apply_local_timeouts(
    conn: psycopg.AsyncConnection,
    *,
    lock_timeout_ms: int,
    statement_timeout_ms: int,
    idle_transaction_timeout_ms: int,
) -> None:
    await conn.execute(
        "SELECT set_config('lock_timeout', %s, true)",
        (f"{lock_timeout_ms}ms",),
    )
    await conn.execute(
        "SELECT set_config('statement_timeout', %s, true)",
        (f"{statement_timeout_ms}ms",),
    )
    await conn.execute(
        "SELECT set_config('idle_in_transaction_session_timeout', %s, true)",
        (f"{idle_transaction_timeout_ms}ms",),
    )


def _plan_cutoff(plan: object) -> datetime:
    try:
        return _require_aware_utc(getattr(plan, "cutoff"))
    except (AttributeError, TypeError):
        raise CheckpointRepositoryError("cleanup_plan_invalid") from None


def _plan_expiry(plan: object) -> datetime:
    try:
        value = getattr(plan, "expires_at")
    except AttributeError:
        raise CheckpointRepositoryError("cleanup_plan_invalid") from None
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise CheckpointRepositoryError("cleanup_plan_invalid")
    try:
        if value.utcoffset() is None:
            raise CheckpointRepositoryError("cleanup_plan_invalid")
    except (OverflowError, TypeError, ValueError):
        raise CheckpointRepositoryError("cleanup_plan_invalid") from None
    return value.astimezone(UTC)


async def _assert_plan_unexpired(
    conn: psycopg.AsyncConnection,
    plan: object,
) -> None:
    expires_at = _plan_expiry(plan)
    row = await (
        await conn.execute(
            "SELECT clock_timestamp() < %s::timestamptz",
            (expires_at,),
        )
    ).fetchone()
    if row is None or row[0] is not True:
        raise CheckpointRepositoryError("cleanup_plan_expired")


def _assert_plan_metadata(plan: object, metadata: _DatabaseMetadata) -> None:
    try:
        database_fingerprint = getattr(plan, "database_fingerprint")
        database_timezone = getattr(plan, "database_timezone")
        alembic_revision = getattr(plan, "alembic_revision")
        checkpoint_revision = getattr(plan, "checkpoint_revision")
    except AttributeError:
        raise CheckpointRepositoryError("cleanup_plan_invalid") from None

    if database_fingerprint != metadata.database_fingerprint:
        raise CheckpointRepositoryError("cleanup_plan_database_mismatch")
    if database_timezone != metadata.database_timezone:
        raise CheckpointRepositoryError("cleanup_plan_timezone_mismatch")
    if alembic_revision != metadata.alembic_revision:
        raise CheckpointRepositoryError("cleanup_plan_schema_mismatch")
    if checkpoint_revision != metadata.checkpoint_revision:
        raise CheckpointRepositoryError("cleanup_plan_checkpoint_mismatch")
    _plan_cutoff(plan)


async def _read_blob_descriptor(
    conn: psycopg.AsyncConnection,
    *,
    thread_id: str,
    channel: str,
    version: str,
) -> tuple[str, int | None, bytes | None] | None:
    row = await (
        await conn.execute(
            """
            SELECT
                type,
                octet_length(blob),
                CASE
                    WHEN octet_length(blob) <= %s THEN blob
                    ELSE NULL
                END
            FROM checkpoint_blobs
            WHERE thread_id = %s
              AND checkpoint_ns = ''
              AND channel = %s
              AND version = %s
            """,
            (MAX_CLEANUP_HANDLE_BLOB_BYTES, thread_id, channel, version),
        )
    ).fetchone()
    if row is None:
        return None
    blob = row[2]
    return (
        str(row[0]),
        int(row[1]) if row[1] is not None else None,
        bytes(blob) if blob is not None else None,
    )


async def _read_blob_metadata(
    conn: psycopg.AsyncConnection,
    *,
    thread_id: str,
    channel: str,
    version: str,
) -> tuple[str, int | None] | None:
    row = await (
        await conn.execute(
            """
            SELECT type, octet_length(blob)
            FROM checkpoint_blobs
            WHERE thread_id = %s
              AND checkpoint_ns = ''
              AND channel = %s
              AND version = %s
            """,
            (thread_id, channel, version),
        )
    ).fetchone()
    if row is None:
        return None
    return (
        str(row[0]),
        int(row[1]) if row[1] is not None else None,
    )


def _decode_bounded_value(
    descriptor: tuple[str, int | None, bytes | None],
) -> tuple[bool, object]:
    type_name, size, blob = descriptor
    if size is None or size > MAX_CLEANUP_HANDLE_BLOB_BYTES:
        return False, None
    if type_name == "null":
        return (size == 0, None)
    if type_name not in {"json", "msgpack"} or blob is None:
        return False, None
    try:
        if type_name == "json":
            value = json.loads(blob)
        else:
            value = ormsgpack.unpackb(blob)
    except (json.JSONDecodeError, UnicodeDecodeError, TypeError, ValueError):
        return False, None
    return True, value


async def _shape_and_handles(
    conn: psycopg.AsyncConnection,
    thread_id: str,
) -> tuple[bool, bool]:
    row = await (
        await conn.execute(_LATEST_CHECKPOINT_SHAPE_SQL, (thread_id,))
    ).fetchone()
    if row is None:
        return False, False
    versions = row[:4]
    if any(not isinstance(version, str) or not version for version in versions):
        return False, False
    email_version, content_version, attachment_version, pdf_version = versions
    (
        email_inline_present,
        email_inline_type,
        email_inline_value,
        content_inline_present,
        attachment_inline_present,
        pdf_inline_present,
        pdf_inline_type,
    ) = row[4:]

    email_descriptor = await _read_blob_metadata(
        conn,
        thread_id=thread_id,
        channel="email_id",
        version=email_version,
    )
    if (
        email_inline_present is not True
        or email_inline_type != "string"
        or email_inline_value != thread_id
        or email_descriptor is not None
    ):
        return False, False

    content_descriptor = await _read_blob_metadata(
        conn,
        thread_id=thread_id,
        channel="content_ref",
        version=content_version,
    )
    if (
        content_inline_present is not False
        or content_descriptor is None
        or content_descriptor[0] not in {"json", "msgpack"}
        or content_descriptor[1] is None
        or not 0 < content_descriptor[1] <= MAX_CLEANUP_HANDLE_BLOB_BYTES
    ):
        return False, False

    attachment_descriptor = await _read_blob_descriptor(
        conn,
        thread_id=thread_id,
        channel="attachment_tokens",
        version=attachment_version,
    )
    if attachment_inline_present is not False or attachment_descriptor is None:
        return False, False
    attachments_ok, attachment_tokens = _decode_bounded_value(attachment_descriptor)

    pdf_descriptor = await _read_blob_metadata(
        conn,
        thread_id=thread_id,
        channel="pdf_token",
        version=pdf_version,
    )
    if (
        pdf_inline_present is not True
        or pdf_inline_type not in {"null", "string"}
        or pdf_descriptor is not None
    ):
        return False, False

    if not attachments_ok:
        return False, False
    handles_empty = (
        type(attachment_tokens) is list
        and attachment_tokens == []
        and pdf_inline_type == "null"
    )
    return True, handles_empty


async def _inventory_digest(
    conn: psycopg.AsyncConnection,
    stats: _ThreadStats,
) -> str | None:
    checkpoint_rows = await (
        await conn.execute(
            """
            SELECT
                checkpoint_ns,
                checkpoint_id,
                CASE
                    WHEN parent_checkpoint_id IS NULL THEN
                        ARRAY['null']::pg_catalog.text[]
                    ELSE
                        ARRAY['text', parent_checkpoint_id]::pg_catalog.text[]
                END,
                CASE
                    WHEN type IS NULL THEN
                        ARRAY['null']::pg_catalog.text[]
                    ELSE
                        ARRAY['text', type]::pg_catalog.text[]
                END,
                octet_length(thread_id)
                    + octet_length(checkpoint_ns)
                    + octet_length(checkpoint_id)
                    + coalesce(octet_length(parent_checkpoint_id), 0)
                    + coalesce(octet_length(type), 0)
                    + octet_length(checkpoint::text)
                    + octet_length(metadata::text),
                checkpoint -> 'channel_versions',
                pg_catalog.encode(
                    pg_catalog.sha256(
                        pg_catalog.convert_to('checkpoint:v1', 'UTF8')
                        || pg_catalog.decode('00', 'hex')
                        || pg_catalog.convert_to(checkpoint::text, 'UTF8')
                    ),
                    'hex'
                ),
                pg_catalog.encode(
                    pg_catalog.sha256(
                        pg_catalog.convert_to('metadata:v1', 'UTF8')
                        || pg_catalog.decode('00', 'hex')
                        || pg_catalog.convert_to(metadata::text, 'UTF8')
                    ),
                    'hex'
                )
            FROM checkpoints AS checkpoint_row
            WHERE thread_id = %s
            ORDER BY checkpoint_ns, checkpoint_id
            """,
            (stats.thread_id,),
        )
    ).fetchall()
    blob_rows = await (
        await conn.execute(
            """
            SELECT
                checkpoint_ns,
                channel,
                version,
                type,
                octet_length(thread_id)
                    + octet_length(checkpoint_ns)
                    + octet_length(channel)
                    + octet_length(version)
                    + octet_length(type)
                    + coalesce(octet_length(blob), 0),
                pg_catalog.encode(
                    pg_catalog.sha256(
                        pg_catalog.convert_to('checkpoint_blob:v1', 'UTF8')
                        || pg_catalog.decode('00', 'hex')
                        || CASE
                            WHEN blob IS NULL THEN
                                pg_catalog.convert_to('null', 'UTF8')
                            ELSE
                                pg_catalog.convert_to('bytes', 'UTF8')
                                || pg_catalog.decode('00', 'hex')
                                || blob
                        END
                    ),
                    'hex'
                )
            FROM checkpoint_blobs AS blob_row
            WHERE thread_id = %s
            ORDER BY checkpoint_ns, channel, version
            """,
            (stats.thread_id,),
        )
    ).fetchall()
    write_rows = await (
        await conn.execute(
            """
            SELECT
                checkpoint_ns,
                checkpoint_id,
                task_id,
                idx,
                channel,
                CASE
                    WHEN type IS NULL THEN
                        ARRAY['null']::pg_catalog.text[]
                    ELSE
                        ARRAY['text', type]::pg_catalog.text[]
                END,
                task_path,
                octet_length(thread_id)
                    + octet_length(checkpoint_ns)
                    + octet_length(checkpoint_id)
                    + octet_length(task_id)
                    + 4
                    + octet_length(channel)
                    + coalesce(octet_length(type), 0)
                    + octet_length(blob)
                    + octet_length(task_path),
                pg_catalog.encode(
                    pg_catalog.sha256(
                        pg_catalog.convert_to('checkpoint_write_blob:v1', 'UTF8')
                        || pg_catalog.decode('00', 'hex')
                        || CASE
                            WHEN blob IS NULL THEN
                                pg_catalog.convert_to('null', 'UTF8')
                            ELSE
                                pg_catalog.convert_to('bytes', 'UTF8')
                                || pg_catalog.decode('00', 'hex')
                                || blob
                        END
                    ),
                    'hex'
                )
            FROM checkpoint_writes AS write_row
            WHERE thread_id = %s
            ORDER BY checkpoint_ns, checkpoint_id, task_id, idx
            """,
            (stats.thread_id,),
        )
    ).fetchall()

    if (
        len(checkpoint_rows) != stats.checkpoint_rows
        or len(blob_rows) != stats.checkpoint_blob_rows
        or len(write_rows) != stats.checkpoint_write_rows
    ):
        return None

    checkpoint_ids = {(str(row[0]), str(row[1])) for row in checkpoint_rows}
    referenced_blobs: set[tuple[str, str, str]] = set()
    for row in checkpoint_rows:
        versions = row[5]
        if not isinstance(versions, Mapping):
            return None
        for channel, version in versions.items():
            if not isinstance(channel, str) or not isinstance(version, str):
                return None
            referenced_blobs.add((str(row[0]), channel, version))
    if any(
        (str(row[0]), str(row[1]), str(row[2])) not in referenced_blobs
        for row in blob_rows
    ):
        return None
    if any((str(row[0]), str(row[1])) not in checkpoint_ids for row in write_rows):
        return None

    inventory = {
        "status": stats.status,
        "updated_at": stats.updated_at.isoformat(),
        "checkpoints": [
            [
                *row[:5],
                str(row[6]),
                str(row[7]),
            ]
            for row in checkpoint_rows
        ],
        "checkpoint_blobs": [list(row) for row in blob_rows],
        "checkpoint_writes": [list(row) for row in write_rows],
    }
    return _canonical_digest(inventory)


async def _inspect_thread(
    conn: psycopg.AsyncConnection,
    stats: _ThreadStats,
    *,
    max_physical_rows: int,
    max_estimated_logical_bytes: int,
) -> _InspectionResult:
    if stats.has_non_default_namespace:
        return _InspectionResult(None, "non_default_namespace")
    if stats.checkpoint_rows == 0:
        return _InspectionResult(None, "missing_checkpoint")
    if (
        stats.total_rows > max_physical_rows
        or stats.estimated_logical_bytes > max_estimated_logical_bytes
    ):
        return _InspectionResult(None, "thread_budget_exceeded")

    inventory_sha256 = await _inventory_digest(conn, stats)
    if inventory_sha256 is None:
        return _InspectionResult(None, "slim_state_unproven")
    slim_state_proven, cleanup_handles_empty = await _shape_and_handles(
        conn, stats.thread_id
    )
    if not slim_state_proven:
        return _InspectionResult(None, "slim_state_unproven")
    if not cleanup_handles_empty:
        return _InspectionResult(None, "cleanup_handles_present")

    try:
        candidate = CleanupCandidate(
            thread_id=stats.thread_id,
            thread_fingerprint=hashlib.sha256(
                stats.thread_id.encode("utf-8")
            ).hexdigest(),
            status=stats.status,
            updated_at=stats.updated_at,
            checkpoint_rows=stats.checkpoint_rows,
            checkpoint_bytes=stats.checkpoint_bytes,
            checkpoint_blob_rows=stats.checkpoint_blob_rows,
            checkpoint_blob_bytes=stats.checkpoint_blob_bytes,
            checkpoint_write_rows=stats.checkpoint_write_rows,
            checkpoint_write_bytes=stats.checkpoint_write_bytes,
            inventory_sha256=inventory_sha256,
            slim_state_proven=True,
            cleanup_handles_empty=True,
        )
    except (UnicodeEncodeError, ValueError):
        return _InspectionResult(None, "inventory_unavailable")
    return _InspectionResult(candidate, None)


class PostgresCheckpointRepository:
    """Read plans and transactionally delete proven checkpoint threads."""

    def __init__(
        self,
        dsn: str,
        *,
        now: Callable[[], datetime] = _utc_now,
        lock_timeout_ms: int = DEFAULT_LOCK_TIMEOUT_MS,
        statement_timeout_ms: int = DEFAULT_STATEMENT_TIMEOUT_MS,
        idle_transaction_timeout_ms: int = DEFAULT_IDLE_TRANSACTION_TIMEOUT_MS,
    ) -> None:
        if not isinstance(dsn, str) or not dsn:
            raise CheckpointRepositoryError("cleanup_repository_failed")
        if type(lock_timeout_ms) is not int or lock_timeout_ms < 1:
            raise CheckpointRepositoryError("cleanup_repository_failed")
        if type(statement_timeout_ms) is not int or statement_timeout_ms < 1:
            raise CheckpointRepositoryError("cleanup_repository_failed")
        if (
            type(idle_transaction_timeout_ms) is not int
            or idle_transaction_timeout_ms < 1
        ):
            raise CheckpointRepositoryError("cleanup_repository_failed")
        self._dsn = dsn
        self._now = now
        self._lock_timeout_ms = lock_timeout_ms
        self._statement_timeout_ms = statement_timeout_ms
        self._idle_transaction_timeout_ms = idle_transaction_timeout_ms

    async def scan_candidates(
        self,
        *,
        cutoff: datetime,
        limit: int,
        max_physical_rows: int,
        max_estimated_logical_bytes: int,
    ) -> CheckpointScanSnapshot:
        now = _require_aware_utc(self._now())
        normalized_cutoff = _validate_scan_inputs(
            cutoff=cutoff,
            now=now,
            limit=limit,
            max_physical_rows=max_physical_rows,
            max_estimated_logical_bytes=max_estimated_logical_bytes,
        )
        counts = {reason: 0 for reason in EXCLUSION_REASONS}
        candidates: list[CleanupCandidate] = []
        aggregate_rows = 0
        aggregate_bytes = 0
        inspection_limit = min(MAX_INSPECTED_THREADS, max(limit * 10, limit))

        try:
            async with await psycopg.AsyncConnection.connect(
                self._dsn,
                autocommit=True,
                prepare_threshold=0,
            ) as conn:
                async with conn.transaction():
                    await conn.execute(
                        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
                    )
                    await _apply_local_timeouts(
                        conn,
                        lock_timeout_ms=self._lock_timeout_ms,
                        statement_timeout_ms=self._statement_timeout_ms,
                        idle_transaction_timeout_ms=self._idle_transaction_timeout_ms,
                    )
                    metadata = await _read_database_metadata(conn)
                    base_row = await (
                        await conn.execute(
                            _BASE_COUNTS_SQL,
                            (normalized_cutoff, normalized_cutoff),
                        )
                    ).fetchone()
                    if base_row is None:
                        raise CheckpointRepositoryError("cleanup_scan_failed")
                    scanned_count = int(base_row[0])
                    counts["status_not_terminal"] = int(base_row[1])
                    counts["invalid_updated_at"] = int(base_row[2])
                    counts["too_recent"] = int(base_row[3])
                    eligible_count = int(base_row[4])
                    rows = await (
                        await conn.execute(
                            _ELIGIBLE_THREAD_STATS_SQL,
                            (normalized_cutoff, inspection_limit),
                        )
                    ).fetchall()
                    if eligible_count > len(rows):
                        counts["plan_budget_exceeded"] += eligible_count - len(rows)

                    for row in rows:
                        try:
                            stats = _stats_from_row(row)
                        except (TypeError, ValueError):
                            counts["inventory_unavailable"] += 1
                            continue
                        inspection = await _inspect_thread(
                            conn,
                            stats,
                            max_physical_rows=max_physical_rows,
                            max_estimated_logical_bytes=max_estimated_logical_bytes,
                        )
                        if inspection.candidate is None:
                            reason = (
                                inspection.exclusion_reason or "inventory_unavailable"
                            )
                            counts[reason] += 1
                            continue
                        candidate = inspection.candidate
                        exceeds_plan = (
                            len(candidates) >= limit
                            or aggregate_rows + candidate.total_rows > max_physical_rows
                            or aggregate_bytes + candidate.estimated_logical_bytes
                            > max_estimated_logical_bytes
                        )
                        if exceeds_plan:
                            counts["plan_budget_exceeded"] += 1
                            continue
                        candidates.append(candidate)
                        aggregate_rows += candidate.total_rows
                        aggregate_bytes += candidate.estimated_logical_bytes
        except CheckpointRepositoryError:
            raise
        except (psycopg.Error, OSError, ValueError, TypeError):
            raise CheckpointRepositoryError("cleanup_scan_failed") from None

        return CheckpointScanSnapshot(
            cutoff=normalized_cutoff,
            database_fingerprint=metadata.database_fingerprint,
            database_timezone=metadata.database_timezone,
            alembic_revision=metadata.alembic_revision,
            checkpoint_revision=metadata.checkpoint_revision,
            scanned_count=scanned_count,
            candidates=tuple(candidates),
            excluded_buckets=tuple(
                ExclusionBucket(reason=reason, count=counts[reason])
                for reason in EXCLUSION_REASONS
            ),
        )

    async def revalidate_candidate(
        self,
        candidate: CleanupCandidate,
        *,
        plan: object,
    ) -> bool:
        try:
            async with await psycopg.AsyncConnection.connect(
                self._dsn,
                autocommit=True,
                prepare_threshold=0,
            ) as conn:
                async with conn.transaction():
                    await conn.execute(
                        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
                    )
                    await _apply_local_timeouts(
                        conn,
                        lock_timeout_ms=self._lock_timeout_ms,
                        statement_timeout_ms=self._statement_timeout_ms,
                        idle_transaction_timeout_ms=self._idle_transaction_timeout_ms,
                    )
                    metadata = await _read_database_metadata(conn)
                    _assert_plan_metadata(plan, metadata)
                    current = await self._revalidate_on_connection(
                        conn,
                        candidate,
                        cutoff=_plan_cutoff(plan),
                    )
                    return current
        except CheckpointRepositoryError:
            raise
        except (psycopg.Error, OSError, ValueError, TypeError):
            raise CheckpointRepositoryError("cleanup_revalidation_failed") from None

    async def _revalidate_on_connection(
        self,
        conn: psycopg.AsyncConnection,
        candidate: CleanupCandidate,
        *,
        cutoff: datetime,
    ) -> bool:
        row = await (
            await conn.execute(_ONE_THREAD_STATS_SQL, (candidate.thread_id,))
        ).fetchone()
        if row is None:
            return False
        try:
            stats = _stats_from_row(row)
        except (TypeError, ValueError):
            return False
        if (
            stats.status not in TERMINAL_CHECKPOINT_STATUSES
            or stats.updated_at >= cutoff
            or stats.updated_at != candidate.updated_at
        ):
            return False
        inspection = await _inspect_thread(
            conn,
            stats,
            max_physical_rows=MAX_PHYSICAL_ROWS,
            max_estimated_logical_bytes=MAX_ESTIMATED_LOGICAL_BYTES,
        )
        return inspection.candidate == candidate

    @asynccontextmanager
    async def execution_session(
        self,
        *,
        plan: object,
    ) -> AsyncIterator["CheckpointExecutionSession"]:
        conn: psycopg.AsyncConnection | None = None
        lock_acquired = False
        try:
            conn = await psycopg.AsyncConnection.connect(
                self._dsn,
                autocommit=True,
                prepare_threshold=0,
            )
            metadata = await _read_database_metadata(conn)
            _assert_plan_metadata(plan, metadata)
            await _assert_plan_unexpired(conn, plan)
            lock_row = await (
                await conn.execute(
                    "SELECT pg_try_advisory_lock(%s)",
                    (CHECKPOINT_MAINTENANCE_LOCK_KEY,),
                )
            ).fetchone()
            lock_acquired = bool(lock_row and lock_row[0])
            if not lock_acquired:
                raise CheckpointRepositoryError("cleanup_lock_unavailable")

            # PostgreSQL releases the runtime's shared advisory lock as soon as
            # its fence backend dies.  The runtime process itself may still be
            # alive until its next bounded monitor probe hard-stops it, so hold
            # the exclusive lock through that complete detection window before
            # exposing a deletion-capable session.
            await asyncio.sleep(DEFAULT_MAINTENANCE_EXCLUSIVE_SETTLE_SECONDS)
            metadata = await _read_database_metadata(conn)
            _assert_plan_metadata(plan, metadata)
            await _assert_plan_unexpired(conn, plan)
            yield CheckpointExecutionSession(
                repository=self,
                conn=conn,
                plan=plan,
            )
        except CheckpointRepositoryError:
            raise
        except (psycopg.Error, OSError, ValueError, TypeError):
            raise CheckpointRepositoryError("cleanup_repository_failed") from None
        finally:
            if conn is not None:
                if lock_acquired:
                    try:
                        await conn.execute(
                            "SELECT pg_advisory_unlock(%s)",
                            (CHECKPOINT_MAINTENANCE_LOCK_KEY,),
                        )
                    except psycopg.Error:
                        pass
                await conn.close()


class CheckpointExecutionSession:
    """One lock-holding cleanup execution against a dedicated connection."""

    def __init__(
        self,
        *,
        repository: PostgresCheckpointRepository,
        conn: psycopg.AsyncConnection,
        plan: object,
    ) -> None:
        self._repository = repository
        self._conn = conn
        self._plan = plan

    async def delete_candidate(
        self,
        candidate: CleanupCandidate,
    ) -> CandidateDeleteResult:
        plan_candidates = getattr(self._plan, "candidates", None)
        if not isinstance(plan_candidates, tuple) or candidate not in plan_candidates:
            raise CheckpointRepositoryError("cleanup_candidate_not_in_plan")

        try:
            async with self._conn.transaction():
                await _apply_local_timeouts(
                    self._conn,
                    lock_timeout_ms=self._repository._lock_timeout_ms,
                    statement_timeout_ms=self._repository._statement_timeout_ms,
                    idle_transaction_timeout_ms=(
                        self._repository._idle_transaction_timeout_ms
                    ),
                )
                metadata = await _read_database_metadata(self._conn)
                _assert_plan_metadata(self._plan, metadata)
                await _assert_plan_unexpired(self._conn, self._plan)
                # PostgreSQL 15 requires a write-capable privilege for row
                # locks and non-ACCESS-SHARE table locks.  The maintenance
                # role intentionally has SELECT only on emails_log, so the
                # The explicit operator quiescence attestation is a manual
                # precondition; the runtime/maintenance advisory fence is the
                # technical write-isolation boundary for this plain read.
                current_email = await (
                    await self._conn.execute(
                        "SELECT id FROM emails_log WHERE id = %s",
                        (candidate.thread_id,),
                    )
                ).fetchone()
                if current_email is None:
                    return CandidateDeleteResult("stale", 0, 0, 0, 0)
                await self._conn.execute(
                    "LOCK TABLE checkpoints, checkpoint_blobs, checkpoint_writes "
                    "IN SHARE ROW EXCLUSIVE MODE"
                )
                current = await self._repository._revalidate_on_connection(
                    self._conn,
                    candidate,
                    cutoff=_plan_cutoff(self._plan),
                )
                if not current:
                    return CandidateDeleteResult("stale", 0, 0, 0, 0)

                await _assert_plan_unexpired(self._conn, self._plan)
                saver = AsyncPostgresSaver(self._conn)
                await saver.adelete_thread(candidate.thread_id)
                remaining = await (
                    await self._conn.execute(
                        """
                        SELECT
                            (SELECT count(*) FROM checkpoints WHERE thread_id = %s),
                            (SELECT count(*) FROM checkpoint_blobs WHERE thread_id = %s),
                            (SELECT count(*) FROM checkpoint_writes WHERE thread_id = %s)
                        """,
                        (
                            candidate.thread_id,
                            candidate.thread_id,
                            candidate.thread_id,
                        ),
                    )
                ).fetchone()
                if remaining is None or any(int(value) != 0 for value in remaining):
                    raise CheckpointRepositoryError(
                        "cleanup_delete_confirmation_failed"
                    )
        except CheckpointRepositoryError:
            raise
        except (psycopg.Error, OSError, RuntimeError, ValueError, TypeError):
            raise CheckpointRepositoryError("cleanup_delete_failed") from None

        return CandidateDeleteResult(
            disposition="deleted",
            checkpoint_rows=candidate.checkpoint_rows,
            checkpoint_blob_rows=candidate.checkpoint_blob_rows,
            checkpoint_write_rows=candidate.checkpoint_write_rows,
            estimated_logical_bytes=candidate.estimated_logical_bytes,
        )
