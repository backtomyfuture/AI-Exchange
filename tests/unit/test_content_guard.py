import pytest

from src.utils.content_guard import ContentGuard


@pytest.mark.asyncio
async def test_normal_chinese_prose_is_not_treated_as_a_set_of_names():
    guard = ContentGuard()
    original = {
        "subject": "请确认收到",
        "body": "请回复确认已收到这封测试邮件。",
        "sender": "sender@example.com",
        "to": ["recipient@example.com"],
        "cc": [],
    }
    draft = "您好！\n感谢您的来信。邮件已经收到，我会尽快确认并处理。"

    issues = await guard.check_hallucination(draft, original)

    assert not [issue for issue in issues if issue["type"] == "unverified_name"]


@pytest.mark.asyncio
async def test_unverified_chinese_addressee_is_still_reported():
    guard = ContentGuard()
    original = {
        "subject": "项目进展",
        "body": "请确认当前进展。",
        "sender": "sender@example.com",
        "to": ["recipient@example.com"],
        "cc": [],
    }

    issues = await guard.check_hallucination(
        "尊敬的张三先生：\n您好！项目进展已收到。",
        original,
    )

    assert [issue for issue in issues if issue["type"] == "unverified_name"] == [
        {
            "type": "unverified_name",
            "claim": "张三",
            "severity": "warning",
        }
    ]


@pytest.mark.asyncio
async def test_source_backed_chinese_addressee_is_allowed():
    guard = ContentGuard()
    original = {
        "subject": "项目进展",
        "body": "请确认当前进展。",
        "sender": "张三 <sender@example.com>",
        "to": ["recipient@example.com"],
        "cc": [],
    }

    issues = await guard.check_hallucination(
        "张三，您好！\n项目进展已收到。",
        original,
    )

    assert not [issue for issue in issues if issue["type"] == "unverified_name"]


@pytest.mark.asyncio
async def test_source_backed_formal_salutation_is_not_double_counted():
    guard = ContentGuard()
    original = {
        "subject": "项目进展",
        "body": "请确认当前进展。",
        "sender": "张三 <sender@example.com>",
        "to": ["recipient@example.com"],
        "cc": [],
    }

    issues = await guard.check_hallucination(
        "尊敬的张三先生：\n您好！项目进展已收到。",
        original,
    )

    assert not [issue for issue in issues if issue["type"] == "unverified_name"]


@pytest.mark.asyncio
async def test_unverified_dates_remain_blocked():
    guard = ContentGuard()
    original = {
        "subject": "请确认收到",
        "body": "请回复确认已收到。",
        "sender": "sender@example.com",
        "to": ["recipient@example.com"],
        "cc": [],
    }

    issues = await guard.check_hallucination(
        "您好！我会在明天完成。",
        original,
    )

    assert {
        "type": "unverified_date",
        "claim": "明天",
        "severity": "warning",
    } in issues
