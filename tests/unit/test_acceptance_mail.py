from unittest.mock import AsyncMock

import pytest

from src.acceptance_mail import AcceptanceMailRejected, send_acceptance_mail_once


@pytest.mark.asyncio
async def test_acceptance_mail_hard_rejects_every_other_recipient(tmp_path) -> None:
    send = AsyncMock()

    with pytest.raises(AcceptanceMailRejected, match="acceptance_recipient_rejected"):
        await send_acceptance_mail_once(
            recipient="someone-else@tianjin-air.com",
            marker=tmp_path / "attempt.json",
            send=send,
        )

    send.assert_not_awaited()
    assert not (tmp_path / "attempt.json").exists()


@pytest.mark.asyncio
async def test_acceptance_mail_claims_before_one_authorized_send(tmp_path) -> None:
    marker = tmp_path / "attempt.json"
    send = AsyncMock(return_value=True)

    await send_acceptance_mail_once(
        recipient="q-fu@tianjin-air.com",
        marker=marker,
        send=send,
    )

    send.assert_awaited_once()
    assert marker.exists()
    with pytest.raises(AcceptanceMailRejected, match="already_attempted"):
        await send_acceptance_mail_once(
            recipient="q-fu@tianjin-air.com",
            marker=marker,
            send=send,
        )
    assert send.await_count == 1
