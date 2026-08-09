#!/usr/bin/env python3
"""Send the single authorized production acceptance message."""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.acceptance_mail import send_acceptance_mail_once  # noqa: E402
from src.config import get_settings  # noqa: E402
from src.utils.exchange_api import ExchangeClient  # noqa: E402


async def _run(recipient: str) -> None:
    client = ExchangeClient(get_settings())
    try:
        await send_acceptance_mail_once(
            recipient=recipient,
            marker=PROJECT_ROOT / "secrets" / "acceptance_mail_attempted.json",
            send=client.send_email,
        )
    finally:
        await client.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--recipient", required=True)
    args = parser.parse_args()
    asyncio.run(_run(args.recipient))


if __name__ == "__main__":
    main()
