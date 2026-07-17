"""Closed resource registry for rendering untrusted email HTML to PDF."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
import hashlib
import re
from types import MappingProxyType
from urllib.parse import urlsplit


MAX_PDF_ASSET_BYTES = 5 * 1024 * 1024
_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
_ALLOWED_MIME_TYPES = frozenset({"image/gif", "image/jpeg", "image/png"})


class PdfResourceRejected(ValueError):
    """Raised when a document tries to load anything outside its asset map."""


@dataclass(frozen=True, slots=True)
class PdfAsset:
    content: bytes
    mime_type: str
    sha256: str


def _detected_image_type(content: bytes) -> str | None:
    if content.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if content.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if content.startswith((b"GIF87a", b"GIF89a")):
        return "image/gif"
    return None


def register_pdf_asset(content: bytes, declared_type: str | None = None) -> PdfAsset:
    """Validate and register one bounded in-memory image asset."""

    if not isinstance(content, bytes) or not content:
        raise PdfResourceRejected("pdf_asset_empty")
    if len(content) > MAX_PDF_ASSET_BYTES:
        raise PdfResourceRejected("pdf_asset_too_large")

    detected = _detected_image_type(content)
    if detected not in _ALLOWED_MIME_TYPES:
        raise PdfResourceRejected("pdf_asset_type_rejected")

    declared = (declared_type or "").partition(";")[0].strip().casefold()
    if declared and declared != detected:
        raise PdfResourceRejected("pdf_asset_type_mismatch")

    digest = hashlib.sha256(content).hexdigest()
    return PdfAsset(content=content, mime_type=detected, sha256=digest)


def pdf_asset_url(asset: PdfAsset) -> str:
    return f"asset://{asset.sha256}"


def _asset_digest_from_url(url: str) -> str:
    try:
        parsed = urlsplit(url)
        port = parsed.port
    except (TypeError, ValueError) as exc:
        raise PdfResourceRejected("pdf_resource_url_invalid") from exc

    if (
        parsed.scheme != "asset"
        or not parsed.netloc
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
        or parsed.username is not None
        or parsed.password is not None
        or port is not None
        or not _DIGEST.fullmatch(parsed.netloc)
    ):
        raise PdfResourceRejected("pdf_resource_scheme_rejected")
    return parsed.netloc


def restricted_url_fetcher(
    url: str,
    assets: Mapping[str, PdfAsset] | None = None,
) -> dict[str, object]:
    """Serve one exact registered asset and reject every other URL scheme."""

    digest = _asset_digest_from_url(url)
    asset = (assets or {}).get(digest)
    if asset is None or asset.sha256 != digest:
        raise PdfResourceRejected("pdf_asset_not_registered")
    if asset.mime_type not in _ALLOWED_MIME_TYPES:
        raise PdfResourceRejected("pdf_asset_type_rejected")
    if not asset.content or len(asset.content) > MAX_PDF_ASSET_BYTES:
        raise PdfResourceRejected("pdf_asset_size_rejected")
    if hashlib.sha256(asset.content).hexdigest() != digest:
        raise PdfResourceRejected("pdf_asset_digest_mismatch")

    return {
        "string": asset.content,
        "mime_type": asset.mime_type,
        "redirected_url": url,
    }


def make_restricted_url_fetcher(
    assets: Mapping[str, PdfAsset] | None = None,
) -> Callable[[str], dict[str, object]]:
    """Freeze an asset map into a WeasyPrint-compatible URL fetcher."""

    frozen = MappingProxyType(dict(assets or {}))

    def fetch(url: str) -> dict[str, object]:
        return restricted_url_fetcher(url, frozen)

    return fetch


__all__ = [
    "MAX_PDF_ASSET_BYTES",
    "PdfAsset",
    "PdfResourceRejected",
    "make_restricted_url_fetcher",
    "pdf_asset_url",
    "register_pdf_asset",
    "restricted_url_fetcher",
]
