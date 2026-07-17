from __future__ import annotations

import ast
from pathlib import Path


_HANDOFF_PRIMITIVES = frozenset({"_lock_quiesced", "_mark_draining", "_insert_current"})


def test_production_cannot_call_transaction_local_handoff_primitives() -> None:
    project_root = Path(__file__).resolve().parents[2]
    allowed = project_root / "src" / "ingestion" / "ownership.py"
    candidates = list((project_root / "src").rglob("*.py"))
    scripts = project_root / "scripts"
    if scripts.is_dir():
        candidates.extend(scripts.rglob("*.py"))

    violations: list[str] = []
    for path in sorted(candidates):
        if path == allowed:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute) and node.attr in _HANDOFF_PRIMITIVES:
                violations.append(
                    f"{path.relative_to(project_root)}:{node.lineno}:{node.attr}"
                )
            elif (
                isinstance(node, ast.Constant)
                and isinstance(node.value, str)
                and node.value in _HANDOFF_PRIMITIVES
            ):
                violations.append(
                    f"{path.relative_to(project_root)}:{node.lineno}:{node.value}"
                )

    assert violations == [], (
        "transaction-local ownership handoff is reserved for the governed "
        f"activation boundary: {violations}"
    )
