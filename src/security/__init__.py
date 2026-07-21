"""Security boundaries shared by runtime, HTTP and Lark ingress."""

from src.security.auth import (
    is_lark_operator_allowed,
    require_metrics_auth,
    validate_runtime_security,
)
from src.security.html import SanitizedHtml, bound_email_html, sanitize_email_html
from src.security.pdf import PdfAsset, PdfResourceRejected, restricted_url_fetcher
from src.security.redaction import exception_type, fingerprint_identifier

__all__ = [
    "exception_type",
    "fingerprint_identifier",
    "PdfAsset",
    "PdfResourceRejected",
    "SanitizedHtml",
    "bound_email_html",
    "is_lark_operator_allowed",
    "require_metrics_auth",
    "restricted_url_fetcher",
    "sanitize_email_html",
    "validate_runtime_security",
]
