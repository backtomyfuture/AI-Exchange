import ast


def _check_no_print_calls(filepath: str):
    """Parse file AST and ensure no bare print() calls exist."""
    with open(filepath, "r", encoding="utf-8") as f:
        tree = ast.parse(f.read())
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            func = node.func
            if isinstance(func, ast.Name) and func.id == "print":
                return False, f"print() found at line {node.lineno}"
    return True, "No print() found"


def test_categorizer_no_print():
    ok, msg = _check_no_print_calls("src/nodes/categorizer.py")
    assert ok, f"categorizer.py: {msg}"


def test_drafter_no_print():
    ok, msg = _check_no_print_calls("src/nodes/drafter.py")
    assert ok, f"drafter.py: {msg}"
