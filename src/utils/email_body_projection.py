from __future__ import annotations

import re
from dataclasses import dataclass

from bs4 import BeautifulSoup


INLINE_IMAGE_PLACEHOLDER = "[内嵌图片]"

_DATA_IMAGE_URI_RE = re.compile(
    r"data:image/[a-z0-9.+-]+"
    r"(?:;[a-z0-9!#$&^_.+-]+(?:=[a-z0-9!#$&^_.+%~-]+)?)*"
    r";base64,[a-z0-9+/=_-]+",
    flags=re.IGNORECASE,
)
_HORIZONTAL_WHITESPACE_RE = re.compile(r"[^\S\n]+")
_PLAIN_QUOTE_BOUNDARY_RE = re.compile(
    r"^(?:"
    r"-{2,}\s*(?:original|forwarded)\s+message\s*-{2,}"
    r"|[-—]{2,}\s*(?:原始邮件|转发邮件)\s*[-—]{2,}"
    r"|begin\s+forwarded\s+message\s*:?"
    r"|在.+写道\s*[:：]"
    r"|on\s+.+\s+wrote\s*:"
    r")$",
    flags=re.IGNORECASE,
)
_HEADER_FIELD_PATTERNS = {
    "from": re.compile(r"^(?:from|发件人)\s*[:：]", re.IGNORECASE),
    "date": re.compile(
        r"^(?:sent|date|发送时间|发送日期|日期)\s*[:：]",
        re.IGNORECASE,
    ),
    "to": re.compile(r"^(?:to|收件人)\s*[:：]", re.IGNORECASE),
    "cc": re.compile(r"^(?:cc|抄送)\s*[:：]", re.IGNORECASE),
    "subject": re.compile(r"^(?:subject|主题)\s*[:：]", re.IGNORECASE),
}


@dataclass(frozen=True, slots=True)
class ModelBodyProjection:
    text: str
    inline_image_count: int
    current_text: str
    quoted_text: str
    has_quoted_history: bool


def _matched_header_fields(lines: list[str]) -> set[str]:
    return {
        name
        for name, pattern in _HEADER_FIELD_PATTERNS.items()
        if any(pattern.match(line.strip()) for line in lines)
    }


def _looks_like_outlook_header(value: str) -> bool:
    matched = _matched_header_fields(value.splitlines())
    return "from" in matched and len(matched) >= 3


def _insert_html_quote_boundary(soup: BeautifulSoup, source: str) -> str | None:
    marker = "__AI_EXCHANGE_QUOTED_HISTORY__"
    while marker in source:
        marker += "_"

    for element in soup.find_all(True):
        classes = {
            str(value).casefold()
            for value in (element.get("class") or [])
        }
        if "gmail_quote" in classes:
            element.insert_before(marker)
            return marker

        style = str(element.get("style") or "").casefold()
        if "border-top" not in style or not _looks_like_outlook_header(
            element.get_text("\n", strip=True)
        ):
            continue
        element.insert_before(marker)
        return marker
    return None


def _find_text_quote_boundary(lines: list[str]) -> int | None:
    for index, line in enumerate(lines):
        if _PLAIN_QUOTE_BOUNDARY_RE.fullmatch(line.strip()) is not None:
            return index

    for index, line in enumerate(lines):
        if _HEADER_FIELD_PATTERNS["from"].match(line.strip()) is None:
            continue
        window = lines[index : index + 40]
        matched = _matched_header_fields(window)
        if (
            "date" in matched
            and ("to" in matched or "cc" in matched)
            and len(matched) >= 3
        ):
            return index
    return None


def _visible_text_and_image_count(
    body: object,
    *,
    separator: str,
    mark_quoted_history: bool = False,
) -> tuple[str, int, str | None]:
    source = body if isinstance(body, str) else str(body or "")
    soup = BeautifulSoup(source, "html.parser")

    for hidden in soup.find_all(("script", "style")):
        hidden.decompose()

    quote_marker = (
        _insert_html_quote_boundary(soup, source)
        if mark_quoted_history
        else None
    )

    inline_image_count = 0
    for image in soup.find_all("img"):
        image.replace_with(f" {INLINE_IMAGE_PLACEHOLDER} ")
        inline_image_count += 1

    visible_text = soup.get_text(separator=separator, strip=True)
    visible_text, plain_uri_count = _DATA_IMAGE_URI_RE.subn(
        INLINE_IMAGE_PLACEHOLDER,
        visible_text,
    )
    inline_image_count += plain_uri_count
    return visible_text, inline_image_count, quote_marker


def _project_single_body_for_model(body: object) -> ModelBodyProjection:
    """Project one complete or provider-supplied body without image bytes."""
    visible_text, inline_image_count, quote_marker = _visible_text_and_image_count(
        body,
        separator="\n",
        mark_quoted_history=True,
    )

    lines = []
    for raw_line in visible_text.splitlines():
        line = _HORIZONTAL_WHITESPACE_RE.sub(" ", raw_line).strip()
        if line:
            lines.append(line)

    html_boundary = lines.index(quote_marker) if quote_marker in lines else None
    text_boundary = (
        _find_text_quote_boundary(lines)
        if html_boundary is None
        else None
    )
    boundary = html_boundary if html_boundary is not None else text_boundary
    if boundary is None:
        current_lines = lines
        quoted_lines: list[str] = []
    else:
        current_lines = lines[:boundary]
        quoted_start = boundary + 1 if html_boundary is not None else boundary
        quoted_lines = lines[quoted_start:]
        lines = [*current_lines, *quoted_lines]

    return ModelBodyProjection(
        text="\n".join(lines),
        inline_image_count=inline_image_count,
        current_text="\n".join(current_lines),
        quoted_text="\n".join(quoted_lines),
        has_quoted_history=boundary is not None,
    )


def project_email_body_for_model(
    body: object,
    *,
    unique_body: object | None = None,
) -> ModelBodyProjection:
    """Return the safest usable current-message projection for model input.

    Exchange ``UniqueBody`` is provider evidence for the current message, but
    it is not guaranteed to exclude nested quoted history.  Parse it through
    the same boundary detector first, and fall back to the complete body when
    it is empty or cannot yield a current-message section.
    """

    if isinstance(unique_body, str) and unique_body.strip():
        try:
            unique_projection = _project_single_body_for_model(unique_body)
        except Exception:
            unique_projection = None
        if unique_projection is not None and unique_projection.current_text:
            return unique_projection
    return _project_single_body_for_model(body)


def project_email_body_for_guard(body: object) -> str:
    """Return a compact visible-text reference for deterministic content checks."""
    visible_text, _, _ = _visible_text_and_image_count(body, separator=" ")
    return _HORIZONTAL_WHITESPACE_RE.sub(" ", visible_text).strip()


__all__ = [
    "INLINE_IMAGE_PLACEHOLDER",
    "ModelBodyProjection",
    "project_email_body_for_guard",
    "project_email_body_for_model",
]
