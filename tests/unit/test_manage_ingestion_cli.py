from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from scripts import manage_ingestion
from src.ingestion.runtime_capability import POLLING_SCHEMA_REVISION


def _policy() -> dict[str, object]:
    entries = (
        ("sync", "create", "create", "full"),
        ("sync", "update", "update", "metadata_only"),
        ("sync", "delete", "delete", "metadata_only"),
    )
    return {
        "schema_version": 1,
        "scopes": [
            {
                "canonical_key": "INBOX",
                "sync_folder": "INBOX",
                "event_policy_matrix": [
                    {
                        "source": source,
                        "raw_event_type": event,
                        "change_kind": kind,
                        "processing_policy": policy,
                    }
                    for source, event, kind, policy in entries
                ],
            }
        ],
    }


def _contract() -> dict[str, object]:
    return {
        "schema_digest": "a" * 64,
        "protocol_version": 1,
        "build_id": "build-1",
        "config_hash": "b" * 64,
        "adapter_hash": "c" * 64,
        "evidence_manifest_hash": "d" * 64,
    }


def _write_json(path: Path, payload: object) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )


def test_cli_exposes_only_the_lean_operator_command_set() -> None:
    parser = manage_ingestion.build_parser()
    subparsers_action = next(
        action for action in parser._actions if action.dest == "command"
    )

    assert set(subparsers_action.choices) == {
        "initialize",
        "status",
        "pause",
        "resume-ingress",
        "requeue",
    }
    with pytest.raises(SystemExit):
        parser.parse_args(["activate"])


@pytest.mark.asyncio
async def test_initialize_dry_run_validates_exact_files_without_database_io(
    tmp_path: Path,
    capsys,
) -> None:
    policy_file = tmp_path / "policy.json"
    contract_file = tmp_path / "contract.json"
    _write_json(policy_file, _policy())
    _write_json(contract_file, _contract())
    args = manage_ingestion.build_parser().parse_args(
        [
            "initialize",
            "--account-id",
            "8",
            "--policy-file",
            str(policy_file),
            "--contract-file",
            str(contract_file),
            "--actor",
            "operator",
            "--reason",
            "fresh install",
            "--idempotency-key",
            "initialize-8",
            "--dry-run",
        ]
    )

    with patch.object(
        manage_ingestion,
        "_open_operator_pool",
        new=AsyncMock(side_effect=AssertionError("dry_run_opened_database")),
    ) as open_pool:
        await manage_ingestion.run(args)

    output = json.loads(capsys.readouterr().out)
    assert output["account_id"] == 8
    assert output["dry_run"] is True
    assert output["schema_revision"] == POLLING_SCHEMA_REVISION
    assert output["scope_count"] == 1
    assert len(output["capability_hash"]) == 64
    assert len(output["policy_manifest_hash"]) == 64
    open_pool.assert_not_awaited()


@pytest.mark.parametrize(
    ("mutation", "error"),
    [
        (lambda value: value.update(schema_version=2), "policy_manifest_invalid"),
        (
            lambda value: value["scopes"][0]["event_policy_matrix"].pop(),
            "policy_manifest_invalid",
        ),
    ],
)
def test_policy_file_fails_closed_on_schema_or_matrix_drift(
    tmp_path: Path,
    mutation,
    error: str,
) -> None:
    value = _policy()
    mutation(value)
    policy_file = tmp_path / "policy.json"
    _write_json(policy_file, value)

    with pytest.raises(manage_ingestion.OperatorCommandError, match=error):
        manage_ingestion._policy_snapshot(str(policy_file))


def test_cli_source_contains_no_cutover_or_compatibility_command() -> None:
    source = Path(manage_ingestion.__file__).read_text(encoding="utf-8")

    for command in (
        'add_parser("activate")',
        'add_parser("backfill")',
        'add_parser("shadow")',
        'add_parser("sync")',
        'add_parser("rollback")',
        'add_parser("migrate-legacy")',
    ):
        assert command not in source
