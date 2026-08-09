from types import SimpleNamespace

import pytest

from src.safety.recipients import (
    ResolvedRecipients,
    normalize_recipient_address,
    resolve_recipient,
    resolve_recipients,
)


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


@pytest.mark.asyncio
async def test_resolve_recipient_accepts_exchange_mailbox_text():
    assert await resolve_recipient(
        "Mailbox(name='财务', email_address='finance@example.com')"
    ) == "finance@example.com"


@pytest.mark.asyncio
async def test_resolve_recipients_normalizes_and_deduplicates_all_inputs():
    def lookup(_request):
        return SimpleNamespace(
            success=lambda: True,
            data=SimpleNamespace(
                user=SimpleNamespace(
                    enterprise_email="finance@example.com",
                    email=None,
                )
            ),
        )

    client = SimpleNamespace(
        contact=SimpleNamespace(
            v3=SimpleNamespace(user=SimpleNamespace(get=lookup)),
        )
    )

    result = await resolve_recipients(
        [
            "Finance <finance@example.com>",
            "open_id=finance-user",
            "finance@example.com",
        ],
        ["Mailbox(name='抄送', email_address='cc@example.com')"],
        lark_client=client,
    )

    assert result == ResolvedRecipients(
        to=("finance@example.com",),
        cc=("cc@example.com",),
    )


@pytest.mark.asyncio
async def test_resolve_recipients_fails_closed_for_invalid_container_or_entry():
    assert await resolve_recipients("recipient@example.com", []) is None
    assert await resolve_recipients(["recipient@example.com", "not-an-address"], []) is None
