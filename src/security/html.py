"""Sanitize untrusted email HTML before it reaches a renderer."""

from __future__ import annotations

from collections.abc import Collection
from dataclasses import dataclass
import re
from urllib.parse import urlsplit

from bs4 import BeautifulSoup
import nh3


MAX_EMAIL_HTML_CHARACTERS = 1_048_576
_ASSET_URL = re.compile(r"asset://([0-9a-f]{64})\Z")
_ALLOWED_TAGS = {
    "a",
    "abbr",
    "b",
    "blockquote",
    "br",
    "code",
    "div",
    "em",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "hr",
    "i",
    "img",
    "li",
    "ol",
    "p",
    "pre",
    "span",
    "strong",
    "table",
    "tbody",
    "td",
    "th",
    "thead",
    "tr",
    "u",
    "ul",
}
_CLEAN_CONTENT_TAGS = {
    "applet",
    "embed",
    "form",
    "iframe",
    "math",
    "noscript",
    "object",
    "script",
    "style",
    "svg",
    "template",
}
_ALLOWED_ATTRIBUTES = {
    "a": {"href", "title"},
    "abbr": {"title"},
    "img": {"alt", "src", "title"},
    "td": {"colspan", "rowspan", "title"},
    "th": {"colspan", "rowspan", "title"},
}


@dataclass(frozen=True, slots=True)
class SanitizedHtml:
    """A safe HTML fragment plus non-sensitive removal statistics."""

    html: str
    removed_elements: int
    removed_attributes: int
    truncated: bool = False


def bound_email_html(raw_html: object) -> tuple[str, bool]:
    """Bound untrusted markup before any HTML parser sees it."""

    source = raw_html if isinstance(raw_html, str) else str(raw_html or "")
    truncated = len(source) > MAX_EMAIL_HTML_CHARACTERS
    return source[:MAX_EMAIL_HTML_CHARACTERS], truncated


def _tag_and_attribute_counts(value: str) -> tuple[int, int]:
    soup = BeautifulSoup(value, "html.parser")
    tags = tuple(soup.find_all(True))
    return len(tags), sum(len(tag.attrs) for tag in tags)


def sanitize_email_html(
    raw_html: str,
    *,
    allowed_asset_ids: Collection[str] = (),
    source_truncated: bool = False,
) -> SanitizedHtml:
    """Return an allowlist-only fragment with no external resource capability.

    Email markup is untrusted. Remote/local URLs, inline CSS, forms, active
    content, event handlers and unknown data URLs are removed. Images survive
    only when their ``asset://`` digest was registered from validated in-memory
    bytes for this exact render.
    """

    source, sanitizer_truncated = bound_email_html(raw_html)
    truncated = source_truncated or sanitizer_truncated

    allowed_assets = frozenset(
        value
        for value in allowed_asset_ids
        if isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value)
    )

    def filter_attribute(tag: str, attribute: str, value: str) -> str | None:
        if tag == "img" and attribute == "src":
            match = _ASSET_URL.fullmatch(value)
            if match is None or match.group(1) not in allowed_assets:
                return None
        if tag == "a" and attribute == "href":
            try:
                parsed = urlsplit(value)
            except ValueError:
                return None
            if parsed.scheme.casefold() == "https":
                if (
                    not parsed.hostname
                    or parsed.username is not None
                    or parsed.password is not None
                ):
                    return None
            elif parsed.scheme.casefold() == "mailto":
                if not parsed.path or "\r" in value or "\n" in value:
                    return None
            else:
                return None
        return value

    before_elements, before_attributes = _tag_and_attribute_counts(source)
    cleaned = nh3.clean(
        source,
        tags=_ALLOWED_TAGS,
        clean_content_tags=_CLEAN_CONTENT_TAGS,
        attributes=_ALLOWED_ATTRIBUTES,
        attribute_filter=filter_attribute,
        strip_comments=True,
        link_rel="noopener noreferrer",
        url_schemes={"asset", "https", "mailto"},
    )

    cleaned_soup = BeautifulSoup(cleaned, "html.parser")
    for image in cleaned_soup.find_all("img"):
        source_value = image.get("src")
        match = (
            _ASSET_URL.fullmatch(source_value)
            if isinstance(source_value, str)
            else None
        )
        if match is None or match.group(1) not in allowed_assets:
            image.decompose()
    cleaned = str(cleaned_soup)

    after_elements, after_attributes = _tag_and_attribute_counts(cleaned)
    if truncated:
        cleaned += "<p><strong>[邮件正文过长，PDF 中已截断]</strong></p>"

    return SanitizedHtml(
        html=cleaned,
        removed_elements=max(0, before_elements - after_elements),
        removed_attributes=max(0, before_attributes - after_attributes),
        truncated=truncated,
    )


__all__ = [
    "MAX_EMAIL_HTML_CHARACTERS",
    "SanitizedHtml",
    "bound_email_html",
    "sanitize_email_html",
]
