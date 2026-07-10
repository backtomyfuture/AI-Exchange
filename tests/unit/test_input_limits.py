from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import ValidationError

from src.config import Settings
from src.safety.input_limits import (
    InputLimitExceeded,
    InputLimits,
    validate_email_input,
)


def test_input_limits_have_locked_defaults():
    assert InputLimits() == InputLimits(
        webhook_bytes=1_048_576,
        exchange_response_bytes=67_108_864,
        body_bytes=10_485_760,
        attachment_count=20,
        attachment_single_bytes=26_214_400,
        attachment_total_bytes=52_428_800,
    )


def test_body_limit_counts_utf8_bytes_and_allows_equality():
    limits = InputLimits(body_bytes=6)

    validate_email_input({"body": "汉汉", "attachments": []}, limits)

    with pytest.raises(InputLimitExceeded) as caught:
        validate_email_input({"body": "汉汉a", "attachments": []}, limits)

    assert caught.value.category == "body_bytes"


def test_attachment_count_is_rejected_before_size_checks():
    limits = InputLimits(
        attachment_count=1,
        attachment_single_bytes=1,
        attachment_total_bytes=1,
    )

    with pytest.raises(InputLimitExceeded) as caught:
        validate_email_input(
            {
                "body": "ok",
                "attachments": [
                    {"content": "not-base64"},
                    {"content": "also-not-base64"},
                ],
            },
            limits,
        )

    assert caught.value.category == "attachment_count"


def test_attachment_total_limit_is_checked_before_decode():
    default_limits = InputLimits()
    encoded = "A" * 70_000_000

    with pytest.raises(InputLimitExceeded) as caught:
        validate_email_input(
            {"body": "ok", "attachments": [{"content": encoded}]},
            default_limits,
        )

    assert caught.value.category == "attachment_total_bytes"


def test_attachment_total_is_checked_before_single_attachment_limit():
    limits = InputLimits(
        attachment_single_bytes=4,
        attachment_total_bytes=5,
    )

    with pytest.raises(InputLimitExceeded) as caught:
        validate_email_input(
            {"body": "ok", "attachments": [{"content": "!!!!!!!!"}]},
            limits,
        )

    assert caught.value.category == "attachment_total_bytes"


def test_declared_attachment_sizes_allow_equality_and_reject_one_byte_over():
    limits = InputLimits(
        attachment_single_bytes=4,
        attachment_total_bytes=8,
    )

    validate_email_input(
        {
            "body": "ok",
            "attachments": [{"size": 4}, {"size": 4}],
        },
        limits,
    )

    with pytest.raises(InputLimitExceeded) as caught:
        validate_email_input(
            {"body": "ok", "attachments": [{"size": 5}]},
            limits,
        )

    assert caught.value.category == "attachment_single_bytes"


def test_encoded_content_takes_precedence_over_declared_size():
    limits = InputLimits(
        attachment_single_bytes=3,
        attachment_total_bytes=3,
    )

    validate_email_input(
        {
            "body": "ok",
            "attachments": [{"content": "!!!!", "size": 10_000}],
        },
        limits,
    )


@pytest.mark.parametrize(
    "field_name",
    [
        "WEBHOOK_MAX_BYTES",
        "EXCHANGE_RESPONSE_MAX_BYTES",
        "EMAIL_BODY_MAX_BYTES",
        "EMAIL_ATTACHMENT_MAX_COUNT",
        "EMAIL_ATTACHMENT_SINGLE_MAX_BYTES",
        "EMAIL_ATTACHMENT_TOTAL_MAX_BYTES",
        "LLM_MAX_INPUT_TOKENS",
        "LLM_MAX_OUTPUT_TOKENS",
        "LLM_MAX_TOTAL_TOKENS",
    ],
)
@pytest.mark.parametrize("invalid_value", [0, -1])
def test_safety_settings_require_positive_integers(
    field_name: str,
    invalid_value: int,
):
    with pytest.raises(ValidationError):
        Settings(_env_file=None, **{field_name: invalid_value})


@pytest.mark.asyncio
@pytest.mark.parametrize("skip_analysis", [False, True], ids=["full", "archive"])
async def test_common_email_boundary_rejects_before_mutation_or_side_effects(
    skip_analysis: bool,
):
    from src.exchange_service import process_and_archive_email

    email = {
        "id": "oversized-email",
        "sender": "sender@example.com",
        "body": "123456",
        "attachments": [{"content": "AAAA"}],
    }
    ctx = MagicMock()
    ctx.db_manager = AsyncMock()
    ctx.email_processor = MagicMock()
    ctx.graph = AsyncMock()
    ctx.exchange_client = AsyncMock()

    with patch("src.exchange_service.get_settings") as mock_settings, patch(
        "src.exchange_service._upload_attachments_to_lark",
        new_callable=AsyncMock,
    ) as mock_upload, patch(
        "src.exchange_service._ingest_to_qdrant",
        new_callable=AsyncMock,
    ) as mock_ingest, patch(
        "src.exchange_service._run_ai_pipeline",
        new_callable=AsyncMock,
    ) as mock_graph:
        mock_settings.return_value = SimpleNamespace(EMAIL_BODY_MAX_BYTES=5)

        with pytest.raises(InputLimitExceeded) as caught:
            await process_and_archive_email(
                email,
                ctx,
                skip_analysis=skip_analysis,
            )

    assert caught.value.category == "body_bytes"
    assert "draft_to" not in email
    assert "draft_cc" not in email
    ctx.db_manager.log_initial_email.assert_not_awaited()
    mock_upload.assert_not_awaited()
    mock_ingest.assert_not_awaited()
    mock_graph.assert_not_awaited()
