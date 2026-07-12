import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from src.maintenance.cleanup_artifacts import (
    ArtifactAlreadyExistsError,
    ArtifactNotFoundError,
    ArtifactSecurityError,
    ArtifactStoreError,
    ArtifactValidationError,
    PlanArtifactStore,
    plan_artifact_sha256,
)
from src.maintenance.cleanup_models import (
    CLEANUP_PLAN_SCHEMA_VERSION,
    CLEANUP_POLICY_VERSION,
    CheckpointCleanupPlan,
    CheckpointCleanupReport,
    CleanupCandidate,
    empty_exclusion_buckets,
)


NOW = datetime(2026, 7, 12, 4, 0, tzinfo=UTC)


def _candidate() -> CleanupCandidate:
    return CleanupCandidate(
        thread_id="private-exchange-thread-id",
        thread_fingerprint="1" * 64,
        status="sent",
        updated_at=NOW - timedelta(days=30),
        checkpoint_rows=1,
        checkpoint_bytes=10,
        checkpoint_blob_rows=2,
        checkpoint_blob_bytes=20,
        checkpoint_write_rows=3,
        checkpoint_write_bytes=30,
        inventory_sha256="2" * 64,
        slim_state_proven=True,
        cleanup_handles_empty=True,
    )


def _plan() -> CheckpointCleanupPlan:
    return CheckpointCleanupPlan(
        schema_version=CLEANUP_PLAN_SCHEMA_VERSION,
        policy_version=CLEANUP_POLICY_VERSION,
        created_at=NOW,
        expires_at=NOW + timedelta(minutes=30),
        cutoff=NOW - timedelta(days=14),
        database_fingerprint="3" * 64,
        database_timezone="UTC",
        alembic_revision="0009_task9_security",
        checkpoint_revision=8,
        limit=100,
        max_physical_rows=500,
        max_estimated_logical_bytes=64 * 1024 * 1024,
        candidates=(_candidate(),),
        excluded_buckets=empty_exclusion_buckets(),
    )


def _report(plan: CheckpointCleanupPlan) -> CheckpointCleanupReport:
    return CheckpointCleanupReport(
        plan_id=plan.plan_id,
        plan_artifact_sha256=plan_artifact_sha256(plan),
        dry_run=True,
        started_at=NOW + timedelta(minutes=1),
        completed_at=NOW + timedelta(minutes=2),
        candidate_count=1,
        processed_count=1,
        skipped_count=0,
        deleted_thread_count=0,
        checkpoint_rows=1,
        checkpoint_blob_rows=2,
        checkpoint_write_rows=3,
        estimated_logical_bytes=60,
        error_code=None,
    )


def _artifact_path(root: Path, plan_id: str, kind: str) -> Path:
    return root / f"{plan_id}.{kind}.json"


def _replace_artifact(path: Path, content: bytes, *, mode: int = 0o600) -> None:
    path.unlink(missing_ok=True)
    path.write_bytes(content)
    path.chmod(mode)


def _exception_chain_text(exc: BaseException) -> str:
    rendered: list[str] = []
    current: BaseException | None = exc
    while current is not None:
        rendered.append(f"{current!s} {current!r}")
        if current.__cause__ is not None:
            current = current.__cause__
        elif not current.__suppress_context__:
            current = current.__context__
        else:
            current = None
    return " ".join(rendered)


def test_save_and_load_plan_uses_private_insert_only_content_addressed_file(
    tmp_path: Path,
) -> None:
    root = tmp_path / "cleanup-artifacts"
    store = PlanArtifactStore(root)
    plan = _plan()

    returned_id = store.save_plan(plan)
    artifact = _artifact_path(root, plan.plan_id, "plan")

    assert returned_id == plan.plan_id
    assert store.load_plan(plan.plan_id) == plan
    assert root.stat().st_mode & 0o777 == 0o700
    assert artifact.stat().st_mode & 0o777 == 0o600
    assert artifact.read_bytes() == json.dumps(
        json.loads(artifact.read_bytes()),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    with pytest.raises(ArtifactAlreadyExistsError):
        store.save_plan(plan)


def test_plan_artifact_hash_is_stable_and_binds_private_payload(tmp_path: Path) -> None:
    store = PlanArtifactStore(tmp_path / "artifacts")
    plan = _plan()

    store.save_plan(plan)
    first_hash = plan_artifact_sha256(plan)
    loaded_hash = plan_artifact_sha256(store.load_plan(plan.plan_id))

    assert first_hash == loaded_hash
    assert len(first_hash) == 64
    assert first_hash != plan.plan_id


def test_load_rejects_duplicate_keys_unknown_fields_and_noncanonical_json(
    tmp_path: Path,
) -> None:
    root = tmp_path / "artifacts"
    store = PlanArtifactStore(root)
    plan = _plan()
    store.save_plan(plan)
    path = _artifact_path(root, plan.plan_id, "plan")
    original = path.read_bytes()

    duplicate = original[:-1] + b',"artifact_type":"checkpoint_cleanup_plan"}'
    _replace_artifact(path, duplicate)
    with pytest.raises(ArtifactValidationError, match="duplicate"):
        store.load_plan(plan.plan_id)

    payload = json.loads(original)
    payload["unexpected"] = True
    _replace_artifact(
        path,
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(),
    )
    with pytest.raises(ArtifactValidationError, match="unknown"):
        store.load_plan(plan.plan_id)

    _replace_artifact(path, json.dumps(json.loads(original), indent=2).encode())
    with pytest.raises(ArtifactValidationError, match="canonical"):
        store.load_plan(plan.plan_id)


def test_parser_error_discards_sensitive_exception_cause(tmp_path: Path) -> None:
    sentinel = "private-thread-id-never-leak"
    root = tmp_path / "artifacts"
    store = PlanArtifactStore(root)
    plan = _plan()
    store.save_plan(plan)
    path = _artifact_path(root, plan.plan_id, "plan")
    duplicate = (
        '{"artifact_type":"checkpoint_cleanup_plan",'
        f'"{sentinel}":1,"{sentinel}":2}}'
    ).encode()
    _replace_artifact(path, duplicate)

    with pytest.raises(ArtifactValidationError) as caught:
        store.load_plan(plan.plan_id)

    assert caught.value.__cause__ is None
    assert sentinel not in _exception_chain_text(caught.value)


def test_artifact_path_error_discards_sensitive_exception_cause(
    tmp_path: Path,
) -> None:
    sentinel = "private-artifact-path-never-leak"
    root = tmp_path / sentinel
    store = PlanArtifactStore(root)
    root.rmdir()

    with pytest.raises(ArtifactStoreError) as caught:
        store.load_plan("8" * 64)

    assert caught.value.__cause__ is None
    assert sentinel not in _exception_chain_text(caught.value)


def test_load_rejects_hash_mismatch_and_oversized_file(tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    store = PlanArtifactStore(root, max_bytes=4096)
    plan = _plan()
    store.save_plan(plan)
    path = _artifact_path(root, plan.plan_id, "plan")
    payload = json.loads(path.read_bytes())
    payload["plan"]["database_timezone"] = "Asia/Shanghai"
    _replace_artifact(
        path,
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(),
    )

    with pytest.raises(ArtifactValidationError, match="hash"):
        store.load_plan(plan.plan_id)

    _replace_artifact(path, b"x" * 4097)
    with pytest.raises(ArtifactValidationError, match="large"):
        store.load_plan(plan.plan_id)


def test_store_rejects_symlink_and_unsafe_modes(tmp_path: Path) -> None:
    real_root = tmp_path / "real"
    real_root.mkdir(mode=0o700)
    symlink_root = tmp_path / "link"
    symlink_root.symlink_to(real_root, target_is_directory=True)
    with pytest.raises(ArtifactSecurityError, match="symlink"):
        PlanArtifactStore(symlink_root)

    unsafe_root = tmp_path / "unsafe"
    unsafe_root.mkdir(mode=0o755)
    with pytest.raises(ArtifactSecurityError, match="mode"):
        PlanArtifactStore(unsafe_root)

    root = tmp_path / "artifacts"
    store = PlanArtifactStore(root)
    plan = _plan()
    store.save_plan(plan)
    path = _artifact_path(root, plan.plan_id, "plan")
    path.chmod(0o644)
    with pytest.raises(ArtifactSecurityError, match="mode"):
        store.load_plan(plan.plan_id)

    target = tmp_path / "target"
    target.write_text("{}")
    target.chmod(0o600)
    path.unlink()
    path.symlink_to(target)
    with pytest.raises(ArtifactSecurityError, match="symlink"):
        store.load_plan(plan.plan_id)


def test_claim_is_insert_only_and_failed_execution_cannot_replay(tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    store = PlanArtifactStore(root)
    plan = _plan()
    store.save_plan(plan)

    claim_hash = store.claim_plan(plan.plan_id, claimed_at=NOW + timedelta(minutes=1))

    assert len(claim_hash) == 64
    assert _artifact_path(root, plan.plan_id, "claim").stat().st_mode & 0o777 == 0o600
    with pytest.raises(ArtifactAlreadyExistsError, match="claimed") as caught:
        store.claim_plan(plan.plan_id, claimed_at=NOW + timedelta(minutes=2))
    assert caught.value.__cause__ is None


def test_claim_rejects_expired_plan_and_naive_time(tmp_path: Path) -> None:
    store = PlanArtifactStore(tmp_path / "artifacts")
    plan = _plan()
    store.save_plan(plan)

    with pytest.raises(ArtifactValidationError, match="window"):
        store.claim_plan(plan.plan_id, claimed_at=plan.expires_at)
    with pytest.raises(ArtifactValidationError, match="window"):
        store.claim_plan(plan.plan_id, claimed_at=plan.expires_at + timedelta(seconds=1))
    with pytest.raises(ArtifactValidationError, match="timezone"):
        store.claim_plan(plan.plan_id, claimed_at=datetime(2026, 7, 12, 4, 1))


def test_dry_run_report_does_not_claim_or_consume_plan(tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    store = PlanArtifactStore(root)
    plan = _plan()
    report = _report(plan)
    store.save_plan(plan)

    report_hash = store.save_report(report)

    assert len(report_hash) == 64
    assert not _artifact_path(root, plan.plan_id, "claim").exists()
    assert (
        _artifact_path(root, plan.plan_id, "dry-run-report").stat().st_mode
        & 0o777
        == 0o600
    )
    with pytest.raises(ArtifactAlreadyExistsError):
        store.save_report(report)

    # A dry run must not consume the plan: a later destructive claim is valid.
    store.claim_plan(plan.plan_id, claimed_at=NOW + timedelta(minutes=2))


def test_execute_report_requires_claim_and_can_follow_dry_run(tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    store = PlanArtifactStore(root)
    plan = _plan()
    dry_run_report = _report(plan)
    execute_report = CheckpointCleanupReport(
        plan_id=plan.plan_id,
        plan_artifact_sha256=plan_artifact_sha256(plan),
        dry_run=False,
        started_at=NOW + timedelta(minutes=2),
        completed_at=NOW + timedelta(minutes=3),
        candidate_count=1,
        processed_count=1,
        skipped_count=0,
        deleted_thread_count=1,
        checkpoint_rows=1,
        checkpoint_blob_rows=2,
        checkpoint_write_rows=3,
        estimated_logical_bytes=60,
        error_code=None,
    )
    store.save_plan(plan)
    store.save_report(dry_run_report)

    with pytest.raises(ArtifactValidationError, match="claim"):
        store.save_report(execute_report)

    store.claim_plan(plan.plan_id, claimed_at=NOW + timedelta(minutes=1))
    report_hash = store.save_report(execute_report)

    assert len(report_hash) == 64
    assert (
        _artifact_path(root, plan.plan_id, "execute-report").stat().st_mode
        & 0o777
        == 0o600
    )
    with pytest.raises(ArtifactAlreadyExistsError):
        store.save_report(execute_report)


def test_report_binds_verified_plan_artifact_hash(tmp_path: Path) -> None:
    plan = _plan()

    wrong_hash_report = CheckpointCleanupReport(
        plan_id=plan.plan_id,
        plan_artifact_sha256="9" * 64,
        dry_run=True,
        started_at=NOW + timedelta(minutes=1),
        completed_at=NOW + timedelta(minutes=2),
        candidate_count=0,
        processed_count=0,
        skipped_count=0,
        deleted_thread_count=0,
        checkpoint_rows=0,
        checkpoint_blob_rows=0,
        checkpoint_write_rows=0,
        estimated_logical_bytes=0,
        error_code="plan_drift",
    )
    other_root = tmp_path / "other-artifacts"
    other = PlanArtifactStore(other_root)
    other.save_plan(plan)
    with pytest.raises(ArtifactValidationError, match="hash"):
        other.save_report(wrong_hash_report)


def test_dry_run_report_must_start_inside_plan_validity_window(tmp_path: Path) -> None:
    store = PlanArtifactStore(tmp_path / "artifacts")
    plan = _plan()
    store.save_plan(plan)
    expired_report = CheckpointCleanupReport(
        plan_id=plan.plan_id,
        plan_artifact_sha256=plan_artifact_sha256(plan),
        dry_run=True,
        started_at=plan.expires_at,
        completed_at=plan.expires_at + timedelta(seconds=1),
        candidate_count=1,
        processed_count=0,
        skipped_count=0,
        deleted_thread_count=0,
        checkpoint_rows=0,
        checkpoint_blob_rows=0,
        checkpoint_write_rows=0,
        estimated_logical_bytes=0,
        error_code="plan_expired",
    )

    with pytest.raises(ArtifactValidationError, match="validity window"):
        store.save_report(expired_report)


def test_store_exposes_verified_plan_artifact_hash_across_process_boundary(
    tmp_path: Path,
) -> None:
    store = PlanArtifactStore(tmp_path / "artifacts")
    plan = _plan()
    store.save_plan(plan)

    assert store.plan_artifact_sha256(plan.plan_id) == plan_artifact_sha256(plan)


def test_load_rejects_missing_or_path_traversal_identifier(tmp_path: Path) -> None:
    store = PlanArtifactStore(tmp_path / "artifacts")

    with pytest.raises(ArtifactNotFoundError):
        store.load_plan("8" * 64)
    with pytest.raises(ArtifactValidationError, match="identifier"):
        store.load_plan("../private")


def test_public_summaries_do_not_disclose_artifact_path_or_private_thread(
    tmp_path: Path,
) -> None:
    root = tmp_path / "private" / "cleanup"
    store = PlanArtifactStore(root)
    plan = _plan()
    report = _report(plan)

    rendered = f"{plan.public_summary()} {report.public_summary()}"

    assert str(root) not in rendered
    assert "private-exchange-thread-id" not in rendered
    assert "postgresql://" not in rendered
    assert not any(str(root) in value for value in (store.save_plan(plan),))
