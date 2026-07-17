from __future__ import annotations

import pytest

from src.security.pdf import (
    PdfResourceRejected,
    pdf_asset_url,
    register_pdf_asset,
    restricted_url_fetcher,
)


@pytest.mark.parametrize(
    "url",
    [
        "https://tracker.example/pixel.png",
        "http://127.0.0.1:8000/metrics",
        "http://169.254.169.254/latest/meta-data",
        "file:///etc/passwd",
        "ftp://example.test/a",
        "data:text/html,<script>alert(1)</script>",
        "asset://" + "a" * 64 + "?query=1",
    ],
)
def test_pdf_fetcher_denies_external_local_and_malformed_resources(url: str):
    with pytest.raises(PdfResourceRejected):
        restricted_url_fetcher(url, {})


def test_pdf_fetcher_serves_only_the_exact_registered_image():
    content = b"\x89PNG\r\n\x1a\n" + b"safe-image"
    asset = register_pdf_asset(content, "image/png")

    result = restricted_url_fetcher(pdf_asset_url(asset), {asset.sha256: asset})

    assert result["string"] == content
    assert result["mime_type"] == "image/png"


def test_pdf_asset_rejects_declared_type_mismatch_and_executable_bytes():
    png = b"\x89PNG\r\n\x1a\n" + b"safe-image"
    with pytest.raises(PdfResourceRejected, match="type_mismatch"):
        register_pdf_asset(png, "image/jpeg")
    with pytest.raises(PdfResourceRejected, match="type_rejected"):
        register_pdf_asset(b"MZ" + b"0" * 64, "image/png")
