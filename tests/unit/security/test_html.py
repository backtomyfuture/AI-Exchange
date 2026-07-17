from __future__ import annotations

import pytest

from src.security.html import sanitize_email_html


@pytest.mark.parametrize(
    "payload",
    [
        '<script>alert(1)</script><p>safe</p>',
        '<img src="https://tracker.example/pixel" onerror="alert(1)">',
        '<a href="javascript:alert(1)">open</a>',
        '<form action="https://evil.example"><input name="secret"></form>',
        '<iframe src="file:///etc/passwd"></iframe>',
        '<style>@import url(https://evil.example/a.css)</style>',
        '<svg><use href="http://169.254.169.254/latest/meta-data"></use></svg>',
    ],
)
def test_untrusted_html_cannot_execute_or_load_external_resources(payload: str):
    result = sanitize_email_html(payload)
    lowered = result.html.casefold()

    for forbidden in (
        "<script",
        "<form",
        "<iframe",
        "javascript:",
        "http://",
        "https://",
        "file:",
        "onerror",
        "@import",
    ):
        assert forbidden not in lowered


def test_safe_structure_and_text_are_preserved():
    result = sanitize_email_html(
        "<p>Hello <strong>安全</strong></p>"
        "<table><tr><td colspan='2'>value</td></tr></table>"
    )

    assert "<strong>安全</strong>" in result.html
    assert '<td colspan="2">value</td>' in result.html


def test_safe_https_and_mailto_links_are_preserved_with_rel():
    result = sanitize_email_html(
        '<a href="https://example.org/path?q=1">web</a>'
        '<a href="mailto:owner@example.org">mail</a>'
    )

    assert 'href="https://example.org/path?q=1"' in result.html
    assert 'href="mailto:owner@example.org"' in result.html
    assert result.html.count('rel="noopener noreferrer"') == 2


def test_image_requires_an_exact_registered_asset_digest():
    digest = "a" * 64
    allowed = sanitize_email_html(
        f'<img src="asset://{digest}" alt="chart">',
        allowed_asset_ids={digest},
    )
    denied = sanitize_email_html(f'<img src="asset://{digest}" alt="chart">')

    assert f"asset://{digest}" in allowed.html
    assert "<img" not in denied.html


def test_body_is_bounded_before_sanitizing():
    result = sanitize_email_html("x" * 1_100_000)

    assert result.truncated is True
    assert "PDF 中已截断" in result.html
