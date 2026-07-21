import base64
import hashlib

from src.utils.email_renderer import render_email_html


def test_render_email_html_preserves_body_and_escapes_envelope_fields():
    rendered = render_email_html(
        {
            "subject": "Quarterly <script>alert(1)</script>",
            "sender": "Sender <sender@example.com>",
            "to": ["Receiver <receiver@example.com>"],
            "cc": ["Copy <copy@example.com>"],
            "body": "<p>Rendered body</p>",
            "received_at": "2026-07-12T08:30:00+08:00",
        }
    )

    assert '<meta name="viewport"' in rendered
    assert "Quarterly &lt;script&gt;alert(1)&lt;/script&gt;" in rendered
    assert "Sender &lt;sender@example.com&gt;" in rendered
    assert "Receiver &lt;receiver@example.com&gt;" in rendered
    assert "Copy &lt;copy@example.com&gt;" in rendered
    assert "<p>Rendered body</p>" in rendered


def test_render_email_html_replaces_known_cid_image_with_registered_asset():
    image = b"\x89PNG\r\n\x1a\n" + b"safe-image"
    encoded = base64.b64encode(image).decode("ascii")
    digest = hashlib.sha256(image).hexdigest()
    rendered = render_email_html(
        {
            "subject": "Inline image",
            "body": '<html><body><img src="cid:chart.png"></body></html>',
            "attachments": [
                {
                    "content_id": "chart.png",
                    "content": encoded,
                }
            ],
        }
    )

    assert "cid:chart.png" not in rendered
    assert f"asset://{digest}" in rendered
    assert rendered.assets[digest].content == image
    assert rendered.count("<html") == 1


def test_render_email_html_preserves_exchange_inline_data_uri_as_registered_asset():
    image = b"\x89PNG\r\n\x1a\n" + b"exchange-inline-image"
    encoded = base64.b64encode(image).decode("ascii")
    digest = hashlib.sha256(image).hexdigest()

    rendered = render_email_html(
        {
            "subject": "Exchange inline image",
            "body": f'<img src="data:image/png;base64,{encoded}">',
            "attachments": [
                {
                    "content_id": "chart.png",
                    "content_type": "image/png",
                    "content": encoded,
                    "is_inline": True,
                }
            ],
        }
    )

    assert "data:image" not in rendered
    assert f"asset://{digest}" in rendered
    assert rendered.assets[digest].content == image


def test_render_email_html_rejects_data_uri_without_matching_attachment():
    image = b"\x89PNG\r\n\x1a\n" + b"unregistered-image"
    encoded = base64.b64encode(image).decode("ascii")

    rendered = render_email_html(
        {
            "subject": "Unregistered inline image",
            "body": f'<p>safe</p><img src="data:image/png;base64,{encoded}">',
            "attachments": [],
        }
    )

    assert "<p>safe</p>" in rendered
    assert "data:image" not in rendered
    assert "<img" not in rendered
    assert not rendered.assets


def test_render_email_html_bounds_body_before_parsing():
    rendered = render_email_html(
        {
            "subject": "Large body",
            "body": "<p>" + ("x" * 1_100_000) + "</p>",
        }
    )

    assert "PDF 中已截断" in rendered
    assert len(rendered) < 1_060_000


def test_render_email_html_strips_active_markup_and_remote_resources():
    rendered = render_email_html(
        {
            "subject": "Malicious",
            "body": (
                '<script>alert(1)</script><img src="http://169.254.169.254/x" '
                'onerror="alert(2)"><a href="javascript:alert(3)">click</a>'
            ),
        }
    )

    lowered = rendered.casefold()
    assert "<script" not in lowered
    assert "169.254.169.254" not in lowered
    assert "javascript:" not in lowered
    assert "onerror" not in lowered
