from pathlib import Path


def test_categorizer_no_inline_retry():
    """categorizer should not define its own retry logic."""
    source = Path("src/nodes/categorizer.py").read_text(encoding="utf-8")
    assert "from tenacity import" not in source, (
        "categorizer.py should use with_llm_retry from retry_decorator, not inline tenacity"
    )


def test_drafter_no_inline_retry():
    """drafter should not define its own retry logic."""
    source = Path("src/nodes/drafter.py").read_text(encoding="utf-8")
    assert "from tenacity import" not in source, (
        "drafter.py should use with_llm_retry from retry_decorator, not inline tenacity"
    )
