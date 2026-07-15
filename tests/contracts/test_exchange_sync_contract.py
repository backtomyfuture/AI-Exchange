from __future__ import annotations

import ast
import dataclasses
import inspect
import textwrap
from pathlib import Path

from src.ingestion.models import SyncBatch
from src.utils.exchange_api import ExchangeClient


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def test_sync_batch_exposes_only_frozen_v2_page_fields() -> None:
    assert [field.name for field in dataclasses.fields(SyncBatch)] == [
        "contract_version",
        "cursor",
        "changes",
        "includes_last",
    ]
    assert SyncBatch.__dataclass_params__.frozen is True
    assert SyncBatch.__slots__ == (
        "contract_version",
        "cursor",
        "changes",
        "includes_last",
    )
    batch = SyncBatch("exchange_sync_contract_v2", "cursor-1", (), False)
    assert not hasattr(batch, "__dict__")


def test_sync_client_has_no_permission_probe_or_test_transport_mutator() -> None:
    assert callable(getattr(ExchangeClient, "sync_emails", None))
    assert not hasattr(ExchangeClient, "validate_sync_permission")
    assert not hasattr(ExchangeClient, "replace_transport_for_test")


def test_sync_client_boundary_imports_no_policy_database_or_repository_layer() -> None:
    source_path = PROJECT_ROOT / "src/utils/exchange_api.py"
    tree = ast.parse(source_path.read_text(encoding="utf-8"), filename=str(source_path))
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module)

    forbidden_prefixes = {
        "src.db",
        "src.ingestion.policy",
        "src.ingestion.repository",
        "src.maintenance.checkpoint_repository",
        "src.utils.db_async",
        "src.utils.notification_policy",
        "psycopg",
        "sqlalchemy",
    }
    assert not any(
        imported_name == prefix or imported_name.startswith(f"{prefix}.")
        for imported_name in imported
        for prefix in forbidden_prefixes
    )


def test_sync_client_remains_dormant_with_no_production_call_sites() -> None:
    call_sites: list[tuple[Path, int]] = []
    for source_path in (PROJECT_ROOT / "src").rglob("*.py"):
        tree = ast.parse(
            source_path.read_text(encoding="utf-8"),
            filename=str(source_path),
        )
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if isinstance(node.func, ast.Name) and node.func.id == "sync_emails":
                call_sites.append((source_path, node.lineno))
            elif isinstance(node.func, ast.Attribute) and node.func.attr == "sync_emails":
                call_sites.append((source_path, node.lineno))

    assert call_sites == []


def _reachable_sync_nodes() -> list[ast.FunctionDef | ast.AsyncFunctionDef]:
    source_path = PROJECT_ROOT / "src/utils/exchange_api.py"
    tree = ast.parse(source_path.read_text(encoding="utf-8"), filename=str(source_path))
    module_functions = {
        node.name: node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    exchange_class = next(
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "ExchangeClient"
    )
    methods = {
        node.name: node
        for node in exchange_class.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }

    pending = [methods["sync_emails"]]
    reachable: list[ast.FunctionDef | ast.AsyncFunctionDef] = []
    seen: set[tuple[str, str]] = set()
    while pending:
        node = pending.pop()
        namespace = "method" if node.name in methods and methods[node.name] is node else "module"
        identity = (namespace, node.name)
        if identity in seen:
            continue
        seen.add(identity)
        reachable.append(node)
        for call in (child for child in ast.walk(node) if isinstance(child, ast.Call)):
            if isinstance(call.func, ast.Name) and call.func.id in module_functions:
                pending.append(module_functions[call.func.id])
            elif (
                isinstance(call.func, ast.Attribute)
                and isinstance(call.func.value, ast.Name)
                and call.func.value.id == "self"
                and call.func.attr in methods
            ):
                pending.append(methods[call.func.attr])
    return reachable


def test_sync_call_graph_has_one_bounded_stream_and_no_unbounded_response_api() -> None:
    reachable = _reachable_sync_nodes()
    stream_calls = [
        node
        for root in reachable
        for node in ast.walk(root)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "stream"
    ]
    bounded_reader_calls = [
        node
        for root in reachable
        for node in ast.walk(root)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "read_json_limited"
    ]
    forbidden_response_access = [
        node
        for root in reachable
        for node in ast.walk(root)
        if isinstance(node, ast.Attribute) and node.attr in {"json", "aread", "text"}
    ]
    loops = [
        node
        for root in reachable
        for node in ast.walk(root)
        if isinstance(node, (ast.For, ast.AsyncFor, ast.While))
    ]

    assert len(stream_calls) == 1
    assert len(bounded_reader_calls) == 1
    assert forbidden_response_access == []
    assert loops == []


def test_strict_reader_uses_incremental_bytearray_and_no_unbounded_response_api() -> None:
    from src.safety.http_response import read_json_limited

    tree = ast.parse(textwrap.dedent(inspect.getsource(read_json_limited)))
    calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)]

    assert any(isinstance(node.func, ast.Name) and node.func.id == "bytearray" for node in calls)
    assert any(
        isinstance(node.func, ast.Attribute) and node.func.attr == "aiter_bytes"
        for node in calls
    )
    assert not any(
        isinstance(node, ast.Attribute) and node.attr in {"json", "aread", "text"}
        for node in ast.walk(tree)
    )
