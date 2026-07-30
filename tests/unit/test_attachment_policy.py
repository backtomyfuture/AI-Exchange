"""AttachmentPolicy 的公开安全准入契约。"""

from __future__ import annotations

import base64
from unittest.mock import patch

import pytest

from src.exchange_service import _upload_attachments_to_lark
from src.nodes.retriever_node import _visual_analysis_inputs
from src.safety.attachments import AttachmentPolicy


def _attachment(
    *,
    name: str,
    content: bytes,
    content_type: str = "application/octet-stream",
) -> dict[str, str]:
    return {
        "name": name,
        "content": base64.b64encode(content).decode("ascii"),
        "content_type": content_type,
    }


def test_allows_pdf_only_when_its_content_matches_the_declared_file_type():
    decision = AttachmentPolicy().assess(
        _attachment(
            name="报价单.pdf",
            content=b"%PDF-1.7\nexample",
            content_type="application/pdf",
        )
    )

    assert decision.allowed is True
    assert decision.reason is None
    assert decision.name == "报价单.pdf"
    assert decision.content == b"%PDF-1.7\nexample"


def test_rejects_disguised_executable_before_it_can_be_uploaded_or_modelled():
    decision = AttachmentPolicy().assess(
        _attachment(
            name="会议纪要.pdf",
            content=b"MZ\x90\x00\x03\x00\x00\x00",
            content_type="application/pdf",
        )
    )

    assert decision.allowed is False
    assert decision.reason == "attachment_signature_mismatch"
    assert decision.content is None


def test_rejects_executable_extensions_even_when_the_content_type_lies():
    decision = AttachmentPolicy().assess(
        _attachment(
            name="invoice.exe",
            content=b"MZ\x90\x00\x03\x00\x00\x00",
            content_type="application/pdf",
        )
    )

    assert decision.allowed is False
    assert decision.reason == "attachment_type_blocked"


def test_rejects_path_like_filename_without_exposing_its_content():
    decision = AttachmentPolicy().assess(
        _attachment(
            name="../private.pdf",
            content=b"%PDF-1.7\nexample",
            content_type="application/pdf",
        )
    )

    assert decision.allowed is False
    assert decision.reason == "attachment_name_invalid"
    assert decision.content is None


def test_accepts_real_png_for_visual_analysis_without_trusting_mime_alone():
    decision = AttachmentPolicy().assess(
        _attachment(
            name="screen.png",
            content=b"\x89PNG\r\n\x1a\nimage-bytes",
            content_type="image/png",
        )
    )

    assert decision.allowed is True
    assert decision.is_image is True


@pytest.mark.asyncio
async def test_delivery_boundary_withholds_disguised_attachment_before_remote_io():
    email = {
        "attachments": [
            _attachment(
                name="meeting.pdf",
                content=b"MZ\x90\x00\x03\x00\x00\x00",
                content_type="application/pdf",
            )
        ]
    }

    with patch("src.exchange_service.lark_app.upload_file_to_drive") as upload:
        result = await _upload_attachments_to_lark(email)

    assert result.tokens == ()
    assert result.links == ()
    upload.assert_not_called()


def test_visual_boundary_does_not_forward_disguised_image_to_a_model():
    inputs = _visual_analysis_inputs(
        {
            "attachments": [
                _attachment(
                    name="chart.png",
                    content=b"MZ\x90\x00\x03\x00\x00\x00",
                    content_type="image/png",
                )
            ]
        }
    )

    assert inputs == []
