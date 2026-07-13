from __future__ import annotations

import base64
import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import psycopg
import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from langgraph.checkpoint.base import empty_checkpoint
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from psycopg import sql

from scripts import checkpoint_cleanup
from src.db.bootstrap import bootstrap_database
from src.maintenance.checkpoint_cleanup import CheckpointCleaner
from src.maintenance.checkpoint_repository import PostgresCheckpointRepository
from src.maintenance.cleanup_artifacts import (
    ArtifactAlreadyExistsError,
    PlanArtifactStore,
)
from src.maintenance.cleanup_backup import (
    Ed25519BackupReceiptVerifier,
    create_signed_backup_receipt,
    create_ed25519_signed_backup_receipt,
)


pytestmark = pytest.mark.asyncio

NOW = datetime.now(UTC).replace(microsecond=0)
CUTOFF = NOW - timedelta(hours=24)
RECEIPT_PRIVATE_SEED = b"\x11" * 32
RECEIPT_PUBLIC_KEY = (
    Ed25519PrivateKey.from_private_bytes(RECEIPT_PRIVATE_SEED)
    .public_key()
    .public_bytes_raw()
)


@pytest.fixture
async def checkpoint_schema(postgres_database_factory):
    schema = postgres_database_factory()
    await bootstrap_database(schema.dsn, **schema.bootstrap_identity)
    return schema


@pytest.fixture
async def checkpoint_auditor(checkpoint_schema):
    return checkpoint_schema.auditor_role, checkpoint_schema.auditor_dsn


async def _insert_proven_thread(
    dsn: str,
    thread_id: str,
    *,
    updated_at: datetime | None = None,
) -> None:
    updated_at = updated_at or (NOW - timedelta(hours=25))
    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn:
        await conn.execute(
            """
            INSERT INTO emails_log (id, status, updated_at)
            VALUES (
                %s,
                'sent',
                %s::timestamptz AT TIME ZONE current_setting('TimeZone')
            )
            """,
            (thread_id, updated_at),
        )
        checkpoint = empty_checkpoint()
        values: dict[str, object] = {
            "email_id": thread_id,
            "content_ref": {
                "account_id": 8,
                "object_id": "00000000-0000-4000-8000-000000000127",
                "key_version": "v1",
                "sha256": "c" * 64,
            },
            "attachment_tokens": [],
            "pdf_token": None,
        }
        versions = {
            channel: f"{checkpoint['id']}:{index}"
            for index, channel in enumerate(values)
        }
        checkpoint["channel_values"] = values
        checkpoint["channel_versions"] = versions
        checkpoint["updated_channels"] = list(values)
        await AsyncPostgresSaver(conn).aput(
            {
                "configurable": {
                    "thread_id": thread_id,
                    "checkpoint_ns": "",
                }
            },
            checkpoint,
            {},
            versions,
        )


async def _checkpoint_table_inventory(dsn: str) -> tuple[tuple[str, int, str], ...]:
    specifications = (
        ("checkpoints", "checkpoint_ns, checkpoint_id"),
        ("checkpoint_blobs", "checkpoint_ns, channel, version"),
        (
            "checkpoint_writes",
            "checkpoint_ns, checkpoint_id, task_id, idx",
        ),
    )
    results: list[tuple[str, int, str]] = []
    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn:
        for table_name, ordering in specifications:
            rows = await (
                await conn.execute(
                    f"SELECT row_to_json(item)::text FROM "
                    f"(SELECT * FROM {table_name} ORDER BY "
                    f"thread_id, {ordering}) AS item"
                )
            ).fetchall()
            digest = hashlib.sha256()
            for row in rows:
                digest.update(str(row[0]).encode("utf-8"))
                digest.update(b"\n")
            results.append((table_name, len(rows), digest.hexdigest()))
    return tuple(results)


def _cleaner(
    dsn: str,
    root: Path,
    *,
    clock: list[datetime],
) -> tuple[CheckpointCleaner, PlanArtifactStore]:
    store = PlanArtifactStore(root)
    cleaner = CheckpointCleaner(
        repository=PostgresCheckpointRepository(
            dsn,
            now=lambda: clock[0],
        ),
        artifact_store=store,
        backup_verifier=Ed25519BackupReceiptVerifier(RECEIPT_PUBLIC_KEY),
        now=lambda: clock[0],
    )
    return cleaner, store


def _receipt(plan, *, backup_id: str, completed_at: datetime) -> str:
    return create_ed25519_signed_backup_receipt(
        private_seed=RECEIPT_PRIVATE_SEED,
        plan_id=plan.plan_id,
        database_fingerprint=plan.database_fingerprint,
        alembic_revision=plan.alembic_revision,
        checkpoint_revision=plan.checkpoint_revision,
        backup_id=backup_id,
        completed_at=completed_at,
        manifest_sha256="d" * 64,
    )


@pytest.mark.integration
async def test_cli_plan_and_execute_use_distinct_roles_and_ed25519_v2(
    checkpoint_schema,
    checkpoint_auditor,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    auditor_role, auditor_dsn = checkpoint_auditor
    await _insert_proven_thread(
        checkpoint_schema.dsn,
        "cli-ed25519-thread",
    )
    state_dir = tmp_path / "state"
    auditor_file = tmp_path / "auditor-dsn"
    maintenance_file = tmp_path / "maintenance-dsn"
    public_key_file = tmp_path / "receipt-public-key"
    for path, content in (
        (auditor_file, auditor_dsn),
        (maintenance_file, checkpoint_schema.maintenance_dsn),
        (
            public_key_file,
            base64.b64encode(RECEIPT_PUBLIC_KEY).decode("ascii"),
        ),
    ):
        path.write_text(content, encoding="utf-8")
        path.chmod(0o600)

    environment = {
        "CHECKPOINT_AUDITOR_DATABASE_URL_FILE": str(auditor_file),
        "CHECKPOINT_MAINTENANCE_DATABASE_URL_FILE": str(maintenance_file),
        "CHECKPOINT_MAINTENANCE_RECEIPT_ED25519_PUBLIC_KEY_FILE": str(public_key_file),
        "POSTGRES_CHECKPOINT_AUDITOR_ROLE": auditor_role,
        "POSTGRES_MAINTENANCE_ROLE": checkpoint_schema.maintenance_role,
        "POSTGRES_RUNTIME_ROLE": checkpoint_schema.runtime_role,
        "POSTGRES_MIGRATION_OWNER_ROLE": checkpoint_schema.migration_role,
        "POSTGRES_SCHEMA": "public",
    }
    for name, value in environment.items():
        monkeypatch.setenv(name, value)

    assert (
        await checkpoint_cleanup.async_main(
            [
                "plan",
                "--older-than-hours",
                "24",
                "--limit",
                "10",
                "--state-dir",
                str(state_dir),
            ]
        )
        == 0
    )
    plan_output = json.loads(capsys.readouterr().out)
    plan_id = plan_output["plan"]["plan_id"]
    plan = PlanArtifactStore(state_dir).load_plan(plan_id)

    legacy_receipt = create_signed_backup_receipt(
        key=b"legacy-hmac-key-is-never-a-production-verifier" * 2,
        plan_id=plan.plan_id,
        database_fingerprint=plan.database_fingerprint,
        alembic_revision=plan.alembic_revision,
        checkpoint_revision=plan.checkpoint_revision,
        backup_id="backup-cli-v2",
        completed_at=datetime.now(UTC),
        manifest_sha256="e" * 64,
    )
    receipt_file = tmp_path / "receipt.json"
    receipt_file.write_text(legacy_receipt, encoding="utf-8")
    receipt_file.chmod(0o600)
    execute_args = [
        "execute",
        "--plan-id",
        plan_id,
        "--confirm-plan-id",
        plan_id,
        "--backup-id",
        "backup-cli-v2",
        "--backup-receipt",
        str(receipt_file),
        "--operator-attests-service-quiesced",
        "--limit",
        "10",
        "--state-dir",
        str(state_dir),
    ]

    assert await checkpoint_cleanup.async_main(execute_args) == 2
    rejected = json.loads(capsys.readouterr().out)
    assert rejected == {"ok": False, "error_code": "backup_receipt_invalid"}
    assert (
        checkpoint_schema.scalar(
            "SELECT count(*) FROM checkpoints WHERE thread_id = 'cli-ed25519-thread'"
        )
        == 1
    )

    receipt_file.write_text(
        create_ed25519_signed_backup_receipt(
            private_seed=RECEIPT_PRIVATE_SEED,
            plan_id=plan.plan_id,
            database_fingerprint=plan.database_fingerprint,
            alembic_revision=plan.alembic_revision,
            checkpoint_revision=plan.checkpoint_revision,
            backup_id="backup-cli-v2",
            completed_at=datetime.now(UTC),
            manifest_sha256="e" * 64,
        ),
        encoding="utf-8",
    )
    receipt_file.chmod(0o600)

    assert await checkpoint_cleanup.async_main(execute_args) == 0
    executed = json.loads(capsys.readouterr().out)
    assert executed["ok"] is True
    assert executed["report"]["deleted_thread_count"] == 1


@pytest.mark.parametrize(
    "revision",
    ["20260710_0002", "20260710_0003", "20260713_0004"],
    ids=["0002-metadata", "0003-metadata", "0004-metadata"],
)
async def test_cleanup_plan_metadata_allowlist_reports_stored_revision_value(
    checkpoint_schema,
    tmp_path: Path,
    revision: str,
) -> None:
    checkpoint_schema.execute(
        "UPDATE alembic_version SET version_num = %s",
        (revision,),
    )
    await _insert_proven_thread(
        checkpoint_schema.dsn,
        f"actual-revision-{revision}",
    )
    cleaner, _ = _cleaner(
        checkpoint_schema.dsn,
        tmp_path / "artifacts",
        clock=[NOW],
    )

    plan = await cleaner.plan(older_than=CUTOFF, limit=1)
    report = await cleaner.run(
        plan.plan_id,
        dry_run=True,
        backup_id=None,
        limit=plan.limit,
    )

    assert plan.alembic_revision == revision
    assert report.dry_run is True
    assert report.deleted_thread_count == 0
    assert report.processed_count == 1


async def test_live_style_dry_run_preserves_all_checkpoint_rows_and_hashes(
    checkpoint_schema,
    tmp_path: Path,
) -> None:
    thread_id = "dry-run-private-thread"
    await _insert_proven_thread(checkpoint_schema.dsn, thread_id)
    clock = [NOW]
    cleaner, store = _cleaner(
        checkpoint_schema.dsn,
        tmp_path / "artifacts",
        clock=clock,
    )
    before = await _checkpoint_table_inventory(checkpoint_schema.dsn)

    plan = await cleaner.plan(older_than=CUTOFF, limit=1)
    report = await cleaner.run(
        plan.plan_id,
        dry_run=True,
        backup_id=None,
        limit=1,
    )

    after = await _checkpoint_table_inventory(checkpoint_schema.dsn)
    assert after == before
    assert report.dry_run is True
    assert report.candidate_count == 1
    assert report.processed_count == 1
    assert report.deleted_thread_count == 0
    assert report.error_code is None
    assert thread_id not in str(plan.public_summary())
    assert thread_id not in str(report.public_summary())
    assert (tmp_path / "artifacts" / f"{plan.plan_id}.plan.json").exists()
    assert (tmp_path / "artifacts" / f"{plan.plan_id}.dry-run-report.json").exists()
    assert not (tmp_path / "artifacts" / f"{plan.plan_id}.claim.json").exists()
    assert store.load_plan(plan.plan_id) == plan


async def test_same_plan_can_dry_run_then_execute_with_backup_and_quiescence(
    checkpoint_schema,
    tmp_path: Path,
) -> None:
    thread_id = "execute-private-thread"
    await _insert_proven_thread(checkpoint_schema.dsn, thread_id)
    clock = [NOW]
    cleaner, _ = _cleaner(
        checkpoint_schema.dsn,
        tmp_path / "artifacts",
        clock=clock,
    )
    plan = await cleaner.plan(older_than=CUTOFF, limit=1)
    dry_report = await cleaner.run(
        plan.plan_id,
        dry_run=True,
        backup_id=None,
        limit=1,
    )

    clock[0] = NOW + timedelta(minutes=5)
    receipt = _receipt(
        plan,
        backup_id="isolated-backup-001",
        completed_at=clock[0],
    )
    execute_report = await cleaner.run(
        plan.plan_id,
        dry_run=False,
        backup_id="isolated-backup-001",
        backup_receipt=receipt,
        service_quiesced=True,
        limit=1,
    )

    assert dry_report.deleted_thread_count == 0
    assert execute_report.error_code is None
    assert execute_report.deleted_thread_count == 1
    assert (
        checkpoint_schema.scalar(
            "SELECT count(*) FROM emails_log WHERE id = %s",
            (thread_id,),
        )
        == 1
    )
    for table_name in ("checkpoints", "checkpoint_blobs", "checkpoint_writes"):
        assert (
            checkpoint_schema.scalar(
                f"SELECT count(*) FROM {table_name} WHERE thread_id = %s",
                (thread_id,),
            )
            == 0
        )
    assert (tmp_path / "artifacts" / f"{plan.plan_id}.claim.json").exists()
    assert (tmp_path / "artifacts" / f"{plan.plan_id}.execute-report.json").exists()

    with pytest.raises(ArtifactAlreadyExistsError):
        await cleaner.run(
            plan.plan_id,
            dry_run=False,
            backup_id="isolated-backup-001",
            backup_receipt=receipt,
            service_quiesced=True,
            limit=1,
        )


async def test_cleaner_consumes_failed_plan_and_preserves_rows_on_delete_error(
    checkpoint_schema,
    tmp_path: Path,
) -> None:
    thread_id = "rollback-through-cleaner"
    await _insert_proven_thread(checkpoint_schema.dsn, thread_id)
    clock = [NOW]
    cleaner, _ = _cleaner(
        checkpoint_schema.dsn,
        tmp_path / "artifacts",
        clock=clock,
    )
    plan = await cleaner.plan(older_than=CUTOFF, limit=1)
    checkpoint_schema.execute(
        """
        CREATE FUNCTION fail_cleanup_blob_delete() RETURNS trigger
        LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected'; END $$
        """
    )
    checkpoint_schema.execute(
        """
        CREATE TRIGGER cleanup_blob_delete_failure
        BEFORE DELETE ON checkpoint_blobs
        FOR EACH STATEMENT EXECUTE FUNCTION fail_cleanup_blob_delete()
        """
    )
    clock[0] = NOW + timedelta(minutes=5)
    receipt = _receipt(
        plan,
        backup_id="isolated-backup-rollback",
        completed_at=clock[0],
    )

    report = await cleaner.run(
        plan.plan_id,
        dry_run=False,
        backup_id="isolated-backup-rollback",
        backup_receipt=receipt,
        service_quiesced=True,
        limit=1,
    )

    assert report.deleted_thread_count == 0
    assert report.error_code == "cleanup_delete_failed"
    assert (
        checkpoint_schema.scalar(
            "SELECT count(*) FROM checkpoints WHERE thread_id = %s",
            (thread_id,),
        )
        == 1
    )
    assert (
        checkpoint_schema.scalar(
            "SELECT count(*) FROM checkpoint_blobs WHERE thread_id = %s",
            (thread_id,),
        )
        == 2
    )
    assert (tmp_path / "artifacts" / f"{plan.plan_id}.claim.json").exists()
    assert (tmp_path / "artifacts" / f"{plan.plan_id}.execute-report.json").exists()

    with pytest.raises(ArtifactAlreadyExistsError):
        await cleaner.run(
            plan.plan_id,
            dry_run=False,
            backup_id="isolated-backup-rollback",
            backup_receipt=receipt,
            service_quiesced=True,
            limit=1,
        )


async def test_plan_and_revalidation_support_fixed_offset_database_timezone(
    checkpoint_schema,
    tmp_path: Path,
) -> None:
    database_name = checkpoint_schema.scalar("SELECT current_database()")
    async with await psycopg.AsyncConnection.connect(
        checkpoint_schema.dsn,
        autocommit=True,
    ) as conn:
        await conn.execute(
            sql.SQL("ALTER DATABASE {} SET timezone TO {}").format(
                sql.Identifier(database_name),
                sql.Literal("+05:30"),
            )
        )
    await _insert_proven_thread(
        checkpoint_schema.dsn,
        "fixed-offset-timezone",
    )
    clock = [NOW]
    cleaner, _ = _cleaner(
        checkpoint_schema.dsn,
        tmp_path / "artifacts",
        clock=clock,
    )

    plan = await cleaner.plan(older_than=CUTOFF, limit=1)
    report = await cleaner.run(
        plan.plan_id,
        dry_run=True,
        backup_id=None,
        limit=1,
    )

    assert ":" in plan.database_timezone
    assert report.processed_count == 1
    assert report.deleted_thread_count == 0
