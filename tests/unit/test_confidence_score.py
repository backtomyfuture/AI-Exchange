import pytest
from src.nodes.categorizer import EmailClassification


def test_confidence_field_present():
    """confidence field is part of the classification model."""
    result = EmailClassification(
        priority="P1",
        need_reply=True,
        intent="咨询",
        summary="test",
        reasoning="test",
        confidence=0.85,
    )
    assert result.confidence == 0.85
    assert "confidence" in result.model_dump()


def test_confidence_bounds():
    """confidence must be between 0.0 and 1.0."""
    with pytest.raises(Exception):
        EmailClassification(
            priority="P1",
            need_reply=True,
            intent="咨询",
            summary="test",
            reasoning="test",
            confidence=1.5,
        )

    with pytest.raises(Exception):
        EmailClassification(
            priority="P1",
            need_reply=True,
            intent="咨询",
            summary="test",
            reasoning="test",
            confidence=-0.1,
        )
