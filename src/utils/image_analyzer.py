"""
延迟图片分析器 (Lazy Image Analyzer)

仅在邮件需要回复 (need_reply=True) 时调用。
提供批量分析、图片压缩、智能采样等优化措施。
"""

import base64
import logging
from io import BytesIO
from typing import List, Dict

from langchain_core.messages import HumanMessage
from tenacity import retry, stop_after_attempt, wait_random_exponential
from openai import RateLimitError, APIError, APIConnectionError

from src.utils.rate_limiter import llm_rate_limiter

logger = logging.getLogger(__name__)

# 常量配置
MAX_IMAGES = 6          # 单封邮件最多分析的图片数
COMPRESS_SIZE = 512     # 压缩目标尺寸 (px)
MAX_TOTAL_CHARS = 5000  # 图片描述总文本上限


def _sample_images(images: List[Dict], max_count: int = MAX_IMAGES) -> List[Dict]:
    """
    智能采样：超过 max_count 时选取代表性图片。
    策略：首张 + 尾张 + 中间均匀采样。
    """
    if len(images) <= max_count:
        return images

    sampled = [images[0]]  # 首张

    # 中间均匀采样
    middle_count = max_count - 2
    step = len(images) / (middle_count + 1)
    for i in range(1, middle_count + 1):
        idx = int(i * step)
        idx = min(idx, len(images) - 2)  # 避免取到最后一张
        if images[idx] not in sampled:
            sampled.append(images[idx])

    sampled.append(images[-1])  # 尾张

    # 去重并补足
    seen = set()
    unique = []
    for img in sampled:
        key = img.get("name", id(img))
        if key not in seen:
            seen.add(key)
            unique.append(img)

    logger.info(f"Sampled {len(unique)} images from {len(images)} total")
    return unique[:max_count]


def _compress_image(base64_content: str, max_size: int = COMPRESS_SIZE) -> str:
    """
    将图片压缩到 max_size x max_size 以内，降低 base64 传输体积。
    需要 Pillow 库。如果 Pillow 不可用，返回原始内容。
    """
    try:
        from PIL import Image

        img_bytes = base64.b64decode(base64_content)
        img = Image.open(BytesIO(img_bytes))

        # 只在需要时压缩
        if max(img.size) <= max_size:
            return base64_content

        img.thumbnail((max_size, max_size))

        # 转为 RGB（处理 RGBA/P 等模式）
        if img.mode not in ("RGB", "L"):
            img = img.convert("RGB")

        buffer = BytesIO()
        img.save(buffer, format="JPEG", quality=60)
        compressed = base64.b64encode(buffer.getvalue()).decode()

        original_kb = len(base64_content) * 3 / 4 / 1024
        compressed_kb = len(compressed) * 3 / 4 / 1024
        logger.debug(f"Image compressed: {original_kb:.0f}KB → {compressed_kb:.0f}KB")

        return compressed
    except ImportError:
        logger.warning("Pillow not installed, skipping image compression")
        return base64_content
    except Exception as exc:
        logger.warning("Image compression failed: error_type=%s", type(exc).__name__)
        return base64_content


async def analyze_images(image_attachments: List[Dict]) -> str:
    """
    批量分析图片附件，返回合并的文字描述。

    流程：
    1. 智能采样（超过 MAX_IMAGES 时）
    2. 压缩每张图片
    3. 一次性发送给 Vision API（批量调用）

    Args:
        image_attachments: 包含 name, content (base64), mime_type 的字典列表

    Returns:
        合并的图片描述文本，空字符串表示无结果
    """
    if not image_attachments:
        return ""

    total_count = len(image_attachments)
    logger.info(f"Starting deferred image analysis: {total_count} image(s)")

    # Step 1: 采样
    sampled = _sample_images(image_attachments)

    # Step 2: 压缩 + 构建批量请求
    content_parts = [{
        "type": "text",
        "text": (
            f"以下有 {len(sampled)} 张图片来自同一封邮件"
            f"{'（共 ' + str(total_count) + ' 张，已采样）' if total_count > len(sampled) else ''}。\n"
            "请为每张图片编号并简要描述其核心内容（每张 80 字以内）。\n"
            "重点提取：文字信息、数据、表格、关键视觉元素。\n"
            "如果图片是签名/Logo等装饰性内容，简单标注即可。"
        )
    }]
    for i, img in enumerate(sampled):
        compressed_content = _compress_image(img["content"])
        mime_type = img.get("mime_type", "image/jpeg")
        # 压缩后统一用 JPEG
        if compressed_content != img["content"]:
            mime_type = "image/jpeg"

        content_parts.append({
            "type": "image_url",
            "image_url": {
                "url": f"data:{mime_type};base64,{compressed_content}",
            }
        })

    message = HumanMessage(content=content_parts)

    # Step 3: 调用 Vision API（带重试）
    from src.providers.factory import get_llm
    llm = get_llm(temperature=0.3)

    @retry(
        wait=wait_random_exponential(multiplier=2, max=60),
        stop=stop_after_attempt(3),
        reraise=True
    )
    async def invoke_with_retry():
        await llm_rate_limiter.acquire()
        return await llm.ainvoke([message])

    try:
        response = await invoke_with_retry()
        result = response.content

        # 截断过长的描述
        if len(result) > MAX_TOTAL_CHARS:
            result = result[:MAX_TOTAL_CHARS] + "\n...[描述已截断]"

        logger.info(f"Image analysis completed: {total_count} images → {len(result)} chars description")
        return result

    except RateLimitError as exc:
        logger.warning("Image analysis rate limited: error_type=%s", type(exc).__name__)
        return f"[图片分析失败: 请求限制，邮件包含 {total_count} 张图片]"
    except APIConnectionError as exc:
        logger.error("Image analysis connection failed: error_type=%s", type(exc).__name__)
        return f"[图片分析失败: 连接错误，邮件包含 {total_count} 张图片]"
    except APIError as exc:
        logger.error("Image analysis API failed: error_type=%s", type(exc).__name__)
        return f"[图片分析失败: API 错误，邮件包含 {total_count} 张图片]"
    except Exception as exc:
        logger.error("Image analysis failed: error_type=%s", type(exc).__name__)
        return "[图片分析失败: 未知错误]"
