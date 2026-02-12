import ast
import inspect

import pytest


def test_no_print_in_exchange_api():
    with open("src/utils/exchange_api.py", "r", encoding="utf-8") as f:
        tree = ast.parse(f.read())
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            func = node.func
            if isinstance(func, ast.Name) and func.id == "print":
                pytest.fail(f"exchange_api.py: print() found at line {node.lineno}")


def test_no_load_dotenv_in_exchange_api():
    import src.utils.exchange_api as mod

    source = inspect.getsource(mod)
    assert "load_dotenv()" not in source, (
        "exchange_api.py should not have load_dotenv() side effect"
    )
