import ast
from pathlib import Path


def _get_function_source(path: str, function_name: str) -> str:
    source = Path(path).read_text(encoding="utf-8")
    tree = ast.parse(source)
    lines = source.splitlines()
    for node in tree.body:
        if isinstance(node, ast.AsyncFunctionDef) and node.name == function_name:
            # end_lineno is available on Python 3.8+
            end = getattr(node, "end_lineno", node.lineno)
            return "\n".join(lines[node.lineno - 1 : end])
    raise AssertionError(f"Function {function_name} not found")


def test_process_function_is_short():
    """process_and_archive_email should be under 60 lines after refactor."""
    source = _get_function_source("src/exchange_service.py", "process_and_archive_email")
    line_count = len(source.strip().split("\n"))
    assert line_count < 60, f"process_and_archive_email is {line_count} lines, should be <60"


def test_helper_functions_exist():
    """Verify sub-functions have been extracted."""
    source = Path("src/exchange_service.py").read_text(encoding="utf-8")
    assert "async def _ingest_to_qdrant(" in source, "Missing _ingest_to_qdrant"
    assert "async def _run_ai_pipeline(" in source, "Missing _run_ai_pipeline"
    assert "async def _dispatch_notification(" in source, "Missing _dispatch_notification"
    assert "async def _mark_email_read(" in source, "Missing _mark_email_read"
