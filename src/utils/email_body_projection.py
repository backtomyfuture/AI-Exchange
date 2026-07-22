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


@dataclass(frozen=True, slots=True)
class ModelBodyProjection:
    text: str
    inline_image_count: int


def project_email_body_for_model(body: object) -> ModelBodyProjection:
    """Return visible email text without transporting inline image bytes."""
    source = body if isinstance(body, str) else str(body or "")
    soup = BeautifulSoup(source, "html.parser")

    for hidden in soup.find_all(("script", "style")):
        hidden.decompose()

    inline_image_count = 0
    for image in soup.find_all("img"):
        image.replace_with(f" {INLINE_IMAGE_PLACEHOLDER} ")
        inline_image_count += 1

    visible_text = soup.get_text(separator="\n", strip=True)
    visible_text, plain_uri_count = _DATA_IMAGE_URI_RE.subn(
        INLINE_IMAGE_PLACEHOLDER,
        visible_text,
    )
    inline_image_count += plain_uri_count

    lines = []
    for raw_line in visible_text.splitlines():
        line = _HORIZONTAL_WHITESPACE_RE.sub(" ", raw_line).strip()
        if line:
            lines.append(line)

    return ModelBodyProjection(
        text="\n".join(lines),
        inline_image_count=inline_image_count,
    )


__all__ = [
    "INLINE_IMAGE_PLACEHOLDER",
    "ModelBodyProjection",
    "project_email_body_for_model",
]
