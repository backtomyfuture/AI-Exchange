from src.utils.card_builder import LarkCardBuilder


def test_build_approval_card_uses_pdf_url_from_dict():
    builder = LarkCardBuilder(lark_api_client=None, exchange_client=None)

    card = builder.build_approval_card(
        email_id="e1",
        draft="ok",
        context=[],
        email_data={"subject": "s", "sender": "a@b.com", "to": [], "cc": []},
        classification={},
        pdf_url={"url": "https://www.feishu.cn/file/abc", "file_token": "abc"},
    )

    contents = []
    urls = []
    for el in card.get("elements", []):
        text = el.get("text")
        if isinstance(text, dict):
            content = text.get("content")
            if content:
                contents.append(content)
        for action in el.get("actions", []):
            if action.get("url"):
                urls.append(action.get("url"))

    assert any("https://www.feishu.cn/file/abc" in c for c in contents)
    assert urls == []
