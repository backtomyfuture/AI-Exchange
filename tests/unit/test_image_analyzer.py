from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from src.utils import image_analyzer


@pytest.mark.asyncio
async def test_empty_image_list_does_not_call_model():
    with patch("src.providers.factory.get_llm") as get_llm:
        result = await image_analyzer.analyze_images([])

    assert result == ""
    get_llm.assert_not_called()


@pytest.mark.asyncio
async def test_visual_summary_is_batched_and_bounded():
    llm = SimpleNamespace(
        ainvoke=AsyncMock(
            return_value=SimpleNamespace(
                content="图" * (image_analyzer.MAX_TOTAL_CHARS + 20),
            )
        )
    )
    images = [
        {"name": "first.png", "content": "Zmlyc3Q=", "mime_type": "image/png"},
        {"name": "second.jpg", "content": "c2Vjb25k", "mime_type": "image/jpeg"},
    ]

    with patch("src.providers.factory.get_llm", return_value=llm), patch.object(
        image_analyzer.llm_rate_limiter,
        "acquire",
        new=AsyncMock(),
    ), patch.object(
        image_analyzer,
        "_compress_image",
        side_effect=lambda content: content,
    ):
        result = await image_analyzer.analyze_images(images)

    assert result.startswith("图" * image_analyzer.MAX_TOTAL_CHARS)
    assert result.endswith("...[描述已截断]")
    request = llm.ainvoke.await_args.args[0]
    assert len(request) == 1
    parts = request[0].content
    assert parts[0]["type"] == "text"
    assert "整体不超过 300 字" in parts[0]["text"]
    assert [part["type"] for part in parts[1:]] == ["image_url", "image_url"]
    assert parts[1]["image_url"]["url"] == "data:image/png;base64,Zmlyc3Q="


@pytest.mark.asyncio
async def test_non_text_model_response_fails_closed_to_safe_summary():
    llm = SimpleNamespace(
        ainvoke=AsyncMock(return_value=SimpleNamespace(content=[{"type": "text"}]))
    )

    with patch("src.providers.factory.get_llm", return_value=llm), patch.object(
        image_analyzer.llm_rate_limiter,
        "acquire",
        new=AsyncMock(),
    ), patch.object(
        image_analyzer,
        "_compress_image",
        side_effect=lambda content: content,
    ):
        result = await image_analyzer.analyze_images(
            [{"name": "one.png", "content": "b25l", "mime_type": "image/png"}]
        )

    assert result == "[图片分析失败: 未知错误]"
