"""Prevent real mailbox data from returning as a tracked test fixture."""

from __future__ import annotations

import re
from email import policy
from email.parser import BytesParser
from email.utils import getaddresses
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
FIXTURES = PROJECT_ROOT / "tests" / "fixtures"
SYNTHETIC_EML = FIXTURES / "synthetic_notification.eml"
CONVERT_SCRIPT = PROJECT_ROOT / "tests" / "convert_eml.py"
CARD_SCRIPT = PROJECT_ROOT / "scripts" / "push_test_card.py"

_ALLOWED_MAIL_FIXTURES = {Path("synthetic_notification.eml")}
_MAIL_ARTIFACT_SUFFIXES = {".eml", ".mbox", ".msg", ".ost", ".pdf", ".pst"}
_IPV4 = re.compile(r"(?<![0-9])(?:[0-9]{1,3}\.){3}[0-9]{1,3}(?![0-9])")


def test_mail_fixture_artifacts_match_exact_allowlist() -> None:
    actual = {
        path.relative_to(FIXTURES)
        for path in FIXTURES.rglob("*")
        if path.is_file() and path.suffix.casefold() in _MAIL_ARTIFACT_SUFFIXES
    }

    assert actual == _ALLOWED_MAIL_FIXTURES


def test_synthetic_mail_fixture_has_only_reserved_identities_and_no_route_data() -> (
    None
):
    assert SYNTHETIC_EML.is_file()
    raw = SYNTHETIC_EML.read_bytes()
    assert len(raw) < 16 * 1024

    message = BytesParser(policy=policy.default).parsebytes(raw)
    assert str(message["Subject"]).startswith("[SYNTHETIC]")
    assert message.get_all("Received", []) == []
    assert message.get_all("X-Originating-IP", []) == []

    addresses = getaddresses(
        message.get_all("From", [])
        + message.get_all("To", [])
        + message.get_all("Cc", [])
    )
    assert addresses
    assert all(address.endswith("@example.test") for _name, address in addresses)
    assert str(message["Message-ID"]).endswith("@example.test>")
    assert _IPV4.search(raw.decode("ascii")) is None

    html_parts = [
        part for part in message.walk() if part.get_content_type() == "text/html"
    ]
    image_parts = [
        part for part in message.walk() if part.get_content_maintype() == "image"
    ]
    assert len(html_parts) == 1
    assert "Synthetic rendering fixture" in html_parts[0].get_content()
    assert len(image_parts) == 1
    image = image_parts[0].get_payload(decode=True)
    assert image is not None
    assert image.startswith(b"\x89PNG\r\n\x1a\n")
    assert len(image) < 256


def test_manual_fixture_tools_reference_only_synthetic_input_and_untracked_output() -> (
    None
):
    scripts = CONVERT_SCRIPT.read_text(encoding="utf-8") + CARD_SCRIPT.read_text(
        encoding="utf-8"
    )

    assert "nas.eml" not in scripts
    assert "nas.pdf" not in scripts
    assert "synthetic_notification.eml" in scripts
    assert "artifacts/manual/synthetic_notification.pdf" in scripts
    assert not (FIXTURES / "synthetic_notification.pdf").exists()
