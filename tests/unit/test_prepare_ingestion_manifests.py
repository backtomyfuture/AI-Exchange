from __future__ import annotations

import json
import os
import stat
import subprocess
from argparse import Namespace
from pathlib import Path

import pytest

from scripts import manage_ingestion
from scripts import prepare_ingestion_manifests as preparer


_REVISION = "1" * 40
_TREE = "2" * 40
_WEBHOOK_ID = "AAMkAGI2-real-opaque-inbox-id"
_BUILD_ID = "release-20260717.1"


def _evidence(
    *,
    build_id: str = _BUILD_ID,
    revision: str = _REVISION,
    check_name: str = "full-test-suite",
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "build_id": build_id,
        "source_revision": revision,
        "checks": [
            {
                "name": check_name,
                "command": ".venv/bin/python -m pytest -q",
                "exit_code": 0,
            }
        ],
        "artifacts": [{"name": "image", "sha256": "a" * 64}],
        "accepted_residual_risks": [],
    }


def _write_json(path: Path, value: object, *, indent: int | None = None) -> None:
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=indent),
        encoding="utf-8",
    )


def _args(evidence_file: Path, output_dir: Path, **overrides: object) -> Namespace:
    values: dict[str, object] = {
        "account_id": 8,
        "webhook_inbox_id": _WEBHOOK_ID,
        "sync_folder": "Inbox",
        "build_id": _BUILD_ID,
        "release_evidence_file": evidence_file,
        "output_dir": output_dir,
    }
    values.update(overrides)
    return Namespace(**values)


def _prepare(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    evidence: dict[str, object] | None = None,
    output_name: str = "generated",
    **overrides: object,
) -> tuple[dict[str, object], dict[str, object], Path]:
    evidence_file = tmp_path / f"evidence-{output_name}.json"
    _write_json(evidence_file, evidence or _evidence())
    output_dir = tmp_path / output_name
    monkeypatch.setattr(
        preparer,
        "_require_clean_git_tree",
        lambda _root: {"revision": _REVISION, "tree": _TREE},
    )
    result = preparer.run(_args(evidence_file, output_dir, **overrides))
    assert result["status"] == "prepared"
    policy = json.loads((output_dir / "POLICY.json").read_text(encoding="utf-8"))
    contract = json.loads((output_dir / "CONTRACT.json").read_text(encoding="utf-8"))
    return policy, contract, output_dir


def test_prepares_exact_seven_item_policy_and_valid_runtime_contract(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    policy, contract, output_dir = _prepare(monkeypatch, tmp_path)

    assert set(policy) == {"schema_version", "scopes"}
    assert policy["schema_version"] == 1
    assert len(policy["scopes"]) == 1
    scope = policy["scopes"][0]
    assert scope["canonical_key"] == "INBOX"
    assert scope["webhook_ids"] == [_WEBHOOK_ID]
    assert scope["sync_folder"] == "Inbox"
    assert len(scope["event_policy_matrix"]) == 7
    assert scope["event_policy_matrix"] == [
        {
            "source": "webhook",
            "raw_event_type": "NewMailEvent",
            "change_kind": "create",
            "processing_policy": "full",
        },
        {
            "source": "webhook",
            "raw_event_type": "CreatedEvent",
            "change_kind": "create",
            "processing_policy": "ignored",
        },
        {
            "source": "webhook",
            "raw_event_type": "ModifiedEvent",
            "change_kind": "update",
            "processing_policy": "metadata_only",
        },
        {
            "source": "webhook",
            "raw_event_type": "DeletedEvent",
            "change_kind": "delete",
            "processing_policy": "metadata_only",
        },
        {
            "source": "sync",
            "raw_event_type": "create",
            "change_kind": "create",
            "processing_policy": "full",
        },
        {
            "source": "sync",
            "raw_event_type": "update",
            "change_kind": "update",
            "processing_policy": "metadata_only",
        },
        {
            "source": "sync",
            "raw_event_type": "delete",
            "change_kind": "delete",
            "processing_policy": "metadata_only",
        },
    ]
    assert set(contract) == {
        "schema_digest",
        "protocol_version",
        "build_id",
        "config_hash",
        "adapter_hash",
        "evidence_manifest_hash",
    }
    assert contract["protocol_version"] == 1
    assert contract["build_id"] == _BUILD_ID
    for name in (
        "schema_digest",
        "config_hash",
        "adapter_hash",
        "evidence_manifest_hash",
    ):
        assert preparer._SHA256.fullmatch(contract[name]) is not None

    snapshot = manage_ingestion._policy_snapshot(str(output_dir / "POLICY.json"))
    runtime_contract = manage_ingestion._runtime_contract(
        str(output_dir / "CONTRACT.json"),
        snapshot,
    )
    assert runtime_contract.build_id == _BUILD_ID


def test_output_is_deterministic_canonical_and_permission_bounded(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _, first_contract, first_dir = _prepare(
        monkeypatch,
        tmp_path,
        output_name="first",
    )
    evidence = _evidence()
    evidence_file = tmp_path / "evidence-second.json"
    evidence_file.write_text(
        json.dumps(evidence, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )
    second_dir = tmp_path / "second"
    monkeypatch.setattr(
        preparer,
        "_require_clean_git_tree",
        lambda _root: {"revision": _REVISION, "tree": _TREE},
    )
    preparer.run(_args(evidence_file, second_dir))

    assert (first_dir / "POLICY.json").read_bytes() == (
        second_dir / "POLICY.json"
    ).read_bytes()
    assert (first_dir / "CONTRACT.json").read_bytes() == (
        second_dir / "CONTRACT.json"
    ).read_bytes()
    assert json.loads((second_dir / "CONTRACT.json").read_bytes()) == first_contract
    assert stat.S_IMODE(first_dir.stat().st_mode) == 0o700
    assert stat.S_IMODE((first_dir / "POLICY.json").stat().st_mode) == 0o600
    assert stat.S_IMODE((first_dir / "CONTRACT.json").stat().st_mode) == 0o600


def test_hashes_bind_their_authoritative_inputs_without_copying_evidence(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _, base, _ = _prepare(monkeypatch, tmp_path, output_name="base")
    changed_evidence = _evidence(check_name="container-smoke")
    _, evidence_changed, evidence_dir = _prepare(
        monkeypatch,
        tmp_path,
        evidence=changed_evidence,
        output_name="evidence-changed",
    )
    _, config_changed, _ = _prepare(
        monkeypatch,
        tmp_path,
        output_name="config-changed",
        account_id=9,
    )

    assert evidence_changed["evidence_manifest_hash"] != base["evidence_manifest_hash"]
    assert evidence_changed["config_hash"] == base["config_hash"]
    assert evidence_changed["schema_digest"] == base["schema_digest"]
    assert evidence_changed["adapter_hash"] == base["adapter_hash"]
    assert config_changed["config_hash"] != base["config_hash"]
    assert config_changed["evidence_manifest_hash"] == base["evidence_manifest_hash"]
    assert config_changed["schema_digest"] == base["schema_digest"]
    assert config_changed["adapter_hash"] == base["adapter_hash"]
    output_bytes = (evidence_dir / "CONTRACT.json").read_bytes()
    assert b"container-smoke" not in output_bytes
    assert b"pytest" not in output_bytes


def test_source_hash_changes_with_committed_tree_identity() -> None:
    first = preparer._committed_tree_manifest(
        {"revision": _REVISION, "tree": _TREE},
        kind="test",
        fixed={"protocol_version": 1},
    )
    second = preparer._committed_tree_manifest(
        {"revision": "3" * 40, "tree": "4" * 40},
        kind="test",
        fixed={"protocol_version": 1},
    )

    assert preparer._domain_hash(b"test\x00", first) != preparer._domain_hash(
        b"test\x00", second
    )


def _git(repository: Path, *arguments: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(repository), *arguments],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def test_real_git_identity_binds_clean_head_tree_and_rejects_all_dirt(
    tmp_path: Path,
) -> None:
    repository = tmp_path / "repository"
    repository.mkdir()
    _git(repository, "init", "--quiet")
    _git(repository, "config", "user.name", "Manifest Test")
    _git(repository, "config", "user.email", "manifest@example.invalid")
    (repository / ".gitignore").write_text("*.ignored\n", encoding="utf-8")
    tracked = repository / "tracked.txt"
    tracked.write_text("first\n", encoding="utf-8")
    _git(repository, "add", ".gitignore", "tracked.txt")
    _git(repository, "commit", "--quiet", "-m", "first")

    first = preparer._require_clean_git_tree(repository)
    assert first == {
        "revision": _git(repository, "rev-parse", "HEAD"),
        "tree": _git(repository, "rev-parse", "HEAD^{tree}"),
    }

    (repository / "allowed.ignored").write_text("ignored\n", encoding="utf-8")
    assert preparer._require_clean_git_tree(repository) == first

    untracked = repository / "untracked.txt"
    untracked.write_text("untracked\n", encoding="utf-8")
    with pytest.raises(preparer.ManifestPreparationError, match="not_clean"):
        preparer._require_clean_git_tree(repository)
    untracked.unlink()

    tracked.write_text("dirty\n", encoding="utf-8")
    with pytest.raises(preparer.ManifestPreparationError, match="not_clean"):
        preparer._require_clean_git_tree(repository)

    _git(repository, "add", "tracked.txt")
    with pytest.raises(preparer.ManifestPreparationError, match="not_clean"):
        preparer._require_clean_git_tree(repository)
    _git(repository, "commit", "--quiet", "-m", "second")
    second = preparer._require_clean_git_tree(repository)
    assert second["revision"] != first["revision"]
    assert second["tree"] != first["tree"]


def test_refuses_existing_output_without_modifying_it(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    evidence_file = tmp_path / "evidence.json"
    _write_json(evidence_file, _evidence())
    output_dir = tmp_path / "existing"
    output_dir.mkdir()
    marker = output_dir / "keep"
    marker.write_text("unchanged", encoding="utf-8")
    monkeypatch.setattr(
        preparer,
        "_require_clean_git_tree",
        lambda _root: {"revision": _REVISION, "tree": _TREE},
    )

    with pytest.raises(preparer.ManifestPreparationError, match="output_dir_exists"):
        preparer.run(_args(evidence_file, output_dir))

    assert marker.read_text(encoding="utf-8") == "unchanged"
    assert sorted(path.name for path in output_dir.iterdir()) == ["keep"]


@pytest.mark.parametrize(
    ("mutation", "error"),
    [
        (
            lambda value: value["checks"][0].update(exit_code=1),
            "release_evidence_not_green",
        ),
        (
            lambda value: value.update(build_id="another-build"),
            "release_evidence_build_mismatch",
        ),
        (
            lambda value: value.update(unexpected=True),
            "release_evidence_invalid",
        ),
        (
            lambda value: value.update(artifacts=[]),
            "release_evidence_invalid",
        ),
    ],
)
def test_release_evidence_fails_closed(
    tmp_path: Path,
    mutation,
    error: str,
) -> None:
    value = _evidence()
    mutation(value)
    evidence_file = tmp_path / "evidence.json"
    _write_json(evidence_file, value)

    with pytest.raises(preparer.ManifestPreparationError, match=error):
        preparer._load_release_evidence(evidence_file, build_id=_BUILD_ID)


def test_release_evidence_rejects_duplicate_json_keys(tmp_path: Path) -> None:
    evidence_file = tmp_path / "evidence.json"
    evidence_file.write_text(
        '{"schema_version":1,"schema_version":1}',
        encoding="utf-8",
    )

    with pytest.raises(
        preparer.ManifestPreparationError,
        match="release_evidence_invalid",
    ):
        preparer._load_release_evidence(evidence_file, build_id=_BUILD_ID)


def test_refuses_placeholder_id_stale_evidence_and_manual_hash_arguments(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    evidence_file = tmp_path / "evidence.json"
    _write_json(evidence_file, _evidence())
    monkeypatch.setattr(
        preparer,
        "_require_clean_git_tree",
        lambda _root: {"revision": "3" * 40, "tree": _TREE},
    )
    with pytest.raises(
        preparer.ManifestPreparationError,
        match="release_evidence_source_mismatch",
    ):
        preparer._prepare_payloads(
            account_id=8,
            webhook_inbox_id=_WEBHOOK_ID,
            sync_folder="Inbox",
            build_id=_BUILD_ID,
            release_evidence_file=evidence_file,
        )

    monkeypatch.setattr(
        preparer,
        "_require_clean_git_tree",
        lambda _root: {"revision": _REVISION, "tree": _TREE},
    )
    with pytest.raises(
        preparer.ManifestPreparationError,
        match="webhook_inbox_id_invalid",
    ):
        preparer._prepare_payloads(
            account_id=8,
            webhook_inbox_id="INBOX",
            sync_folder="Inbox",
            build_id=_BUILD_ID,
            release_evidence_file=evidence_file,
        )

    parser = preparer.build_parser()
    options = {option for action in parser._actions for option in action.option_strings}
    assert options.isdisjoint(
        {
            "--schema-digest",
            "--config-hash",
            "--adapter-hash",
            "--evidence-manifest-hash",
        }
    )


def test_partial_output_write_is_fully_removed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "partial"
    original = preparer._write_new_file

    def fail_second(path: Path, payload: bytes) -> None:
        if path.name == "POLICY.json":
            original(path, payload)
            return
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            os.write(descriptor, b"partial")
        finally:
            os.close(descriptor)
        raise OSError("simulated write failure")

    monkeypatch.setattr(preparer, "_write_new_file", fail_second)
    with pytest.raises(preparer.ManifestPreparationError, match="output_write_failed"):
        preparer._write_output_directory(
            output_dir,
            policy={"schema_version": 1},
            contract={"schema_version": 1},
        )

    assert not output_dir.exists()
