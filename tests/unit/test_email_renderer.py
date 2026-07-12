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


def test_render_email_html_replaces_known_cid_image_with_inline_data():
    rendered = render_email_html(
        {
            "subject": "Inline image",
            "body": '<html><body><img src="cid:chart.png"></body></html>',
            "attachments": [
                {
                    "content_id": "chart.png",
                    "content": "aW1hZ2UtYnl0ZXM=",
                }
            ],
        }
    )

    assert "cid:chart.png" not in rendered
    assert "data:image/png;base64,aW1hZ2UtYnl0ZXM=" in rendered
    assert rendered.count("<html") == 1
