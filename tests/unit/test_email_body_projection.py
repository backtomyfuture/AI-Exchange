from src.utils.email_body_projection import project_email_body_for_model


def test_model_projection_keeps_visible_text_without_inline_image_bytes():
    payload = "A" * 4096
    body = (
        "<html><body><p>请结合下图审批。</p>"
        f'<img alt="月度趋势" src="data:image/png;base64,{payload}">'
        "<p>正文结论：建议通过。</p>"
        f"data:image/jpeg;base64,{payload}"
        "</body></html>"
    )

    projection = project_email_body_for_model(body)

    assert "请结合下图审批。" in projection.text
    assert "正文结论：建议通过。" in projection.text
    assert projection.text.count("[内嵌图片]") == 2
    assert "data:image" not in projection.text
    assert payload not in projection.text
    assert projection.inline_image_count == 2
    assert len(projection.text.encode("utf-8")) < 256
