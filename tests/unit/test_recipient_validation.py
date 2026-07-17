import pytest

from src.safety.recipients import normalize_recipient_address


@pytest.mark.parametrize(
    "address",
    [
        f"{'a' * 65}@example.com",
        f"user@{'a' * 64}.com",
        f"user@{'.'.join(['a' * 63] * 4)}",
    ],
)
def test_recipient_rejects_rfc_component_length_overflow(address):
    assert normalize_recipient_address(address) is None


@pytest.mark.parametrize(
    "address",
    [
        f"{'a' * 64}@example.com",
        f"user@{'a' * 63}.com",
    ],
)
def test_recipient_accepts_component_length_boundaries(address):
    assert normalize_recipient_address(address) == address
