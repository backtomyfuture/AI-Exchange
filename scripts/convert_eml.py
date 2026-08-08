"""Manually render the governed synthetic EML fixture to an ignored PDF."""

from __future__ import annotations

from email import policy
from email.parser import BytesParser
from pathlib import Path

from src.utils.email_renderer import render_email_html
from src.utils.pdf_generator import convert_html_to_pdf


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_EML = PROJECT_ROOT / "tests/fixtures/synthetic_notification.eml"
DEFAULT_OUTPUT = PROJECT_ROOT / "artifacts/manual/synthetic_notification.pdf"


def convert_eml(eml_path: str | Path, output_path: str | Path) -> None:
    """Render one local synthetic fixture without retaining mailbox data."""

    source = Path(eml_path)
    target = Path(output_path)
    print(f"Reading {source}...")
    with source.open("rb") as file_obj:
        message = BytesParser(policy=policy.default).parse(file_obj)

    body_html = ""
    if message.is_multipart():
        for part in message.walk():
            disposition = str(part.get("Content-Disposition"))
            if (
                part.get_content_type() == "text/html"
                and "attachment" not in disposition
            ):
                body_html = part.get_content()
                break
    else:
        body_html = message.get_content()

    recipient = str(message.get("To") or "")
    copied = str(message.get("Cc") or "")
    email_data = {
        "subject": str(message.get("Subject") or ""),
        "sender": str(message.get("From") or ""),
        "to": recipient.split(", ") if recipient else [],
        "cc": copied.split(", ") if copied else [],
        "received_at": str(message.get("Date") or ""),
        "body": body_html,
        "attachments": [],
    }

    print("Rendering HTML...")
    html_content = render_email_html(email_data)
    print("Generating PDF with WeasyPrint...")
    pdf_bytes = convert_html_to_pdf(html_content)

    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(pdf_bytes)
    print(f"Saved PDF to {target}")


if __name__ == "__main__":
    convert_eml(DEFAULT_EML, DEFAULT_OUTPUT)
