from src.utils.email_body_projection import (
    project_email_body_for_guard,
    project_email_body_for_model,
)


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


def test_guard_projection_keeps_inline_text_in_a_single_reference_line():
    projection = project_email_body_for_guard(
        "<p><span>7</span><span>月</span><span>30</span><span>日</span></p>"
    )

    assert projection == "7 月 30 日"


def test_model_projection_prefers_usable_exchange_unique_body():
    projection = project_email_body_for_model(
        """
        <p>完整正文中的旧请求：请重新编制预算。</p>
        <div class="gmail_quote"><p>更早的历史。</p></div>
        """,
        unique_body="<p>本轮只需确认收到。</p>",
    )

    assert projection.current_text == "本轮只需确认收到。"
    assert "完整正文中的旧请求" not in projection.text
    assert projection.has_quoted_history is False


def test_model_projection_falls_back_when_unique_body_has_no_current_text():
    projection = project_email_body_for_model(
        """
        <p>本轮材料已完成，请查收。</p>
        <div class="gmail_quote"><p>旧任务：请修改材料。</p></div>
        """,
        unique_body='<div class="gmail_quote"><p>旧任务：请修改材料。</p></div>',
    )

    assert projection.current_text == "本轮材料已完成，请查收。"
    assert projection.has_quoted_history is True
    assert "旧任务：请修改材料。" in projection.quoted_text


def test_model_projection_separates_latest_reply_from_outlook_quoted_history():
    body = """
    <html>
      <body>
        <div class="WordSection1">
          <p>呈阅</p>
          <p>&nbsp;</p>
          <div style="border:none;border-top:solid #E1E1E1 1.0pt;padding:3.0pt 0cm 0cm 0cm">
            <p><b>发件人:</b> 数字化安全管理平台信箱</p>
            <p><b>发送时间:</b> 2026年7月28日 23:12</p>
            <p><b>收件人:</b> 信息技术部</p>
            <p><b>主题:</b> 外部信息单据待处理提醒</p>
            <p>请及时填写信息评估结论。</p>
          </div>
        </div>
      </body>
    </html>
    """

    projection = project_email_body_for_model(body)

    assert projection.current_text == "呈阅"
    assert projection.has_quoted_history is True
    assert "外部信息单据待处理提醒" in projection.quoted_text
    assert "请及时填写信息评估结论。" in projection.quoted_text
    assert "呈阅" in projection.text
    assert "请及时填写信息评估结论。" in projection.text


def test_model_projection_separates_plain_text_original_message():
    body = """收到，已完成。

-----Original Message-----
From: service@example.com
Sent: Tuesday, July 28, 2026 23:12
To: team@example.com
Subject: Pending assessment

Please complete the assessment.
"""

    projection = project_email_body_for_model(body)

    assert projection.current_text == "收到，已完成。"
    assert projection.has_quoted_history is True
    assert "Pending assessment" in projection.quoted_text
    assert "Please complete the assessment." in projection.quoted_text


def test_model_projection_separates_gmail_quote_without_text_delimiter():
    body = """
    <html>
      <body>
        <p>请继续修改第三段。</p>
        <div class="gmail_quote">
          <p>初稿已经完成，请确认。</p>
        </div>
      </body>
    </html>
    """

    projection = project_email_body_for_model(body)

    assert projection.current_text == "请继续修改第三段。"
    assert projection.has_quoted_history is True
    assert "初稿已经完成，请确认。" in projection.quoted_text


def test_model_projection_uses_date_and_cc_header_cluster_as_quote_boundary():
    body = """Latest update: the revised contract is attached.

From: sender@example.com
Date: July 30, 2026 at 10:30 AM
To: owner@example.com; reviewer@example.com
Cc: legal@example.com; finance@example.com

Please revise the contract and send it back.
"""

    projection = project_email_body_for_model(body)

    assert projection.current_text == "Latest update: the revised contract is attached."
    assert projection.has_quoted_history is True
    assert projection.quoted_text.startswith("From: sender@example.com")
    assert "Please revise the contract and send it back." in projection.quoted_text


def test_model_projection_uses_chinese_date_and_cc_header_cluster():
    body = """本轮意见：合同可以提交。

发件人：项目经理
日期：2026年7月30日 10:30
收件人：合同负责人；法务负责人
抄送：财务负责人；档案管理员

请修改合同后重新提交。
"""

    projection = project_email_body_for_model(body)

    assert projection.current_text == "本轮意见：合同可以提交。"
    assert projection.has_quoted_history is True
    assert projection.quoted_text.startswith("发件人：项目经理")
    assert "请修改合同后重新提交。" in projection.quoted_text


def test_model_projection_does_not_split_header_words_used_as_ordinary_prose():
    body = """From experience, this rollout needs one more review.
Sentences in the second section should be shorter.
To improve clarity, move the conclusion to the top.
Subject matter experts can approve the final wording.
"""

    projection = project_email_body_for_model(body)

    assert projection.current_text == (
        "From experience, this rollout needs one more review.\n"
        "Sentences in the second section should be shorter.\n"
        "To improve clarity, move the conclusion to the top.\n"
        "Subject matter experts can approve the final wording."
    )
    assert projection.has_quoted_history is False
    assert projection.quoted_text == ""


def test_model_projection_does_not_treat_business_field_list_as_email_header():
    body = """请按以下范围完成系统迁移：
From: legacy order service
To: unified order service
Subject: historical order data
完成后提交验证报告。
"""

    projection = project_email_body_for_model(body)

    assert projection.current_text == (
        "请按以下范围完成系统迁移：\n"
        "From: legacy order service\n"
        "To: unified order service\n"
        "Subject: historical order data\n"
        "完成后提交验证报告。"
    )
    assert projection.has_quoted_history is False
