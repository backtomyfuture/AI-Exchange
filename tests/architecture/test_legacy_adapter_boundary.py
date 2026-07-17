from __future__ import annotations

import ast
import inspect
from pathlib import Path

from src.exchange_service import (
    WebhookWorker,
    process_and_archive_email_guarded,
)
from src.ingestion.legacy_adapter import LegacyProcessingAdapter
from src.ingestion.processing import ExternalEffectKind


ROOT = Path(__file__).resolve().parents[2]
EXCHANGE_SERVICE = ROOT / "src" / "exchange_service.py"
LEGACY_ADAPTER = ROOT / "src" / "ingestion" / "legacy_adapter.py"
RUNTIME = ROOT / "src" / "ingestion" / "runtime.py"
LIVE_ROOTS = (
    ROOT / "src" / "main.py",
    ROOT / "src" / "server.py",
    ROOT / "src" / "init_app.py",
    ROOT / "src" / "exchange_service.py",
    ROOT / "src" / "scheduler",
)
DIRECT_EFFECT_FUNCTIONS = frozenset(
    {
        "_upload_attachments_to_lark",
        "_ingest_to_qdrant",
        "_run_ai_pipeline",
        "_delete_drive_token_or_retain",
        "_delete_replaced_pdf",
        "_dispatch_notification",
        "_mark_email_read",
        "_delete_unclaimed_content_candidate",
        "_ensure_durable_content_ref",
        "_cleanup_graph_drive_files",
    }
)


def _python_files(path: Path):
    if path.is_file():
        yield path
        return
    yield from sorted(path.rglob("*.py"))


def _tree(path: Path) -> ast.Module:
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def _method_node(tree: ast.Module, class_name: str, method_name: str):
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            for item in node.body:
                if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)) and (
                    item.name == method_name
                ):
                    return item
    raise AssertionError(f"missing {class_name}.{method_name}")


def test_guarded_entry_and_adapter_require_injected_async_effect_port() -> None:
    guarded = inspect.signature(process_and_archive_email_guarded)
    guarded_port = guarded.parameters["before_external_effect"]
    guarded_scope = guarded.parameters["effect_scope"]
    adapter_port = inspect.signature(LegacyProcessingAdapter.process).parameters[
        "before_external_effect"
    ]

    assert guarded_port.kind is inspect.Parameter.KEYWORD_ONLY
    assert guarded_port.default is inspect.Parameter.empty
    assert guarded_scope.kind is inspect.Parameter.KEYWORD_ONLY
    assert guarded_scope.default is inspect.Parameter.empty
    assert adapter_port.kind is inspect.Parameter.KEYWORD_ONLY
    assert adapter_port.default is inspect.Parameter.empty


def test_adapter_default_is_only_the_guarded_legacy_entry() -> None:
    default = (
        inspect.signature(LegacyProcessingAdapter)
        .parameters["guarded_processor"]
        .default
    )
    tree = _tree(LEGACY_ADAPTER)
    imports = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module == "src.exchange_service"
        for alias in node.names
    }

    assert default is process_and_archive_email_guarded
    assert imports == {"process_and_archive_email_guarded"}
    assert not any(isinstance(node, ast.Lambda) for node in ast.walk(tree))


def test_legacy_adapter_has_exactly_one_runtime_factory_wiring() -> None:
    violations: list[str] = []
    for root in (*LIVE_ROOTS, RUNTIME):
        for path in _python_files(root):
            source = path.read_text(encoding="utf-8")
            if (
                "src.ingestion.legacy_adapter" in source
                or "LegacyProcessingAdapter" in source
            ):
                violations.append(str(path.relative_to(ROOT)))

    assert violations == ["src/ingestion/runtime.py"]
    tree = _tree(RUNTIME)
    constructors = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "LegacyProcessingAdapter"
    ]
    assert len(constructors) == 1


def test_webhook_worker_remains_on_unchanged_unguarded_live_entry() -> None:
    tree = _tree(EXCHANGE_SERVICE)
    process_one = _method_node(tree, WebhookWorker.__name__, "_process_one")
    called_names = {
        node.func.id
        for node in ast.walk(process_one)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }

    assert "process_and_archive_email" in called_names
    assert "process_and_archive_email_guarded" not in called_names


def test_every_legacy_external_call_owner_has_an_effect_authorization_gate() -> None:
    tree = _tree(EXCHANGE_SERVICE)
    functions = {
        node.name: node
        for node in tree.body
        if isinstance(node, ast.AsyncFunctionDef)
        and node.name in DIRECT_EFFECT_FUNCTIONS
    }

    assert frozenset(functions) == DIRECT_EFFECT_FUNCTIONS
    for name, function in functions.items():
        calls = {
            node.func.id
            for node in ast.walk(function)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
        }
        assert "_authorize_external_effect" in calls, name


def test_synchronous_lark_network_calls_run_off_the_event_loop() -> None:
    tree = _tree(EXCHANGE_SERVICE)
    threaded_targets = {
        node.args[0].attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "asyncio"
        and node.func.attr == "to_thread"
        and node.args
        and isinstance(node.args[0], ast.Attribute)
        and isinstance(node.args[0].value, ast.Name)
        and node.args[0].value.id == "lark_app"
    }

    assert {
        "upload_file_to_drive",
        "send_approval_card",
        "send_read_only_card",
    } <= threaded_targets


def test_phase2_adapter_effect_ceilings_are_closed_and_exact() -> None:
    from src.ingestion import legacy_adapter

    assert legacy_adapter._FULL_EFFECTS == frozenset(ExternalEffectKind)
    assert legacy_adapter._ARCHIVE_EFFECTS == frozenset(
        {
            ExternalEffectKind.DETAIL,
            ExternalEffectKind.CONTENT,
            ExternalEffectKind.QDRANT,
        }
    )
