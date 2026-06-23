"""
Pure helpers for the Lark approval-card recipient editor.

Originally inlined inside ``lark_app.handle_card_action``. Extracted so they
can be unit-tested without booting the Lark SDK and so the giant action
dispatcher stays readable.
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, Iterable, List, Sequence


_EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")
_EMAIL_FULL_RE = re.compile(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$")


def merge_unique(values: Iterable[Any]) -> List[str]:
    """Return the input list with duplicates and empty strings removed (order preserved)."""
    out: List[str] = []
    seen: set = set()
    for raw in values or []:
        s = str(raw).strip()
        if s and s not in seen:
            out.append(s)
            seen.add(s)
    return out


def read_selected_open_ids(action_data: Any) -> List[str]:
    """
    Parse selected open_ids from a Lark person-picker callback payload.

    The SDK sometimes returns ``options=[uid1, uid2]`` (multi-select) and
    sometimes ``option=uid`` (single-select). We accept both.
    """
    selected: List[str] = []

    options = getattr(action_data, "options", None)
    if isinstance(options, list):
        selected.extend([str(uid).strip() for uid in options if str(uid).strip()])

    option = getattr(action_data, "option", None)
    if option:
        selected.append(str(option).strip())

    return merge_unique(selected)


def normalize_uid_list(raw_value: Any) -> List[str]:
    """
    Coerce a form field value to an open_id list.

    Accepts list/tuple/set, JSON-encoded list strings, or comma-separated text.
    """
    if raw_value is None:
        return []

    values: List[str]
    if isinstance(raw_value, (list, tuple, set)):
        values = [str(x).strip() for x in raw_value]
    elif isinstance(raw_value, str):
        raw = raw_value.strip()
        if not raw:
            values = []
        elif raw.startswith("[") and raw.endswith("]"):
            try:
                parsed = json.loads(raw)
                values = [str(x).strip() for x in parsed] if isinstance(parsed, list) else []
            except Exception:
                values = [v.strip() for v in raw.split(",")]
        else:
            values = [v.strip() for v in raw.split(",")]
    else:
        values = [str(raw_value).strip()]

    return merge_unique(values)


def normalize_email_list(raw_value: Any) -> List[str]:
    """
    Extract clean email addresses from free text.

    Supports common separators (``,;；，`` and whitespace) and ``Name <email>``
    notation. Filters out anything that does not match a basic email regex.
    """
    if raw_value is None:
        return []

    if isinstance(raw_value, (list, tuple, set)):
        raw_text = ",".join(str(x) for x in raw_value if str(x).strip())
    else:
        raw_text = str(raw_value or "")

    matches = _EMAIL_RE.findall(raw_text)
    if matches:
        return merge_unique(matches)

    tokens = re.split(r"[,;；，\s]+", raw_text.strip())
    valid = [t.strip() for t in tokens if t.strip() and _EMAIL_FULL_RE.match(t.strip())]
    return merge_unique(valid)


def extract_external_emails_from_recipients(recipients: Any) -> List[str]:
    """
    Pull out external (non-open_id) email addresses from a recipient list.

    Each entry may be:
        - ``open_id=<uid>`` (skipped, internal user)
        - ``"name='X', email_address='x@y'"`` (legacy Exchange dump)
        - ``"Name <x@y>"``
        - a bare email
        - free text containing one or more emails
    """
    values = recipients or []
    if isinstance(values, str):
        values = [values]

    extracted: List[str] = []
    for item in values:
        text = str(item).strip()
        if not text or text.startswith("open_id="):
            continue

        m = re.search(r"email_address='(.*?)'", text)
        if m:
            extracted.append(m.group(1).strip())
            continue

        m2 = re.search(r"<([^>]+)>", text)
        if m2:
            extracted.append(m2.group(1).strip())
            continue

        if "@" in text and " " not in text:
            extracted.append(text)
            continue

        extracted.extend(normalize_email_list(text))

    return merge_unique(extracted)


def clear_recipient_edit_temp(email_payload: Dict[str, Any], field_type: str) -> None:
    """Drop the temporary ``draft_<field>_*`` keys produced during edit/search."""
    for key in (
        f"draft_{field_type}_options",
        f"draft_{field_type}_search_hint",
        f"draft_{field_type}_new_selected",
        f"draft_{field_type}_external_input",
    ):
        email_payload.pop(key, None)


def merge_keep_and_add(keep_uids: Sequence[str], add_uids: Sequence[str]) -> List[str]:
    """Combine the 'kept existing' and 'newly added' uid lists, preserving order."""
    return merge_unique(list(keep_uids) + list(add_uids))


def build_recipient_field(uids: Sequence[str], external_emails: Sequence[str]) -> List[str]:
    """
    Compose the final recipient list (open_id=uid entries followed by emails),
    de-duplicating across both sets.
    """
    merged: List[str] = []
    seen: set = set()
    for uid in uids:
        marker = f"open_id={uid}"
        if marker not in seen:
            merged.append(marker)
            seen.add(marker)
    for email in external_emails:
        if email and email not in seen:
            merged.append(email)
            seen.add(email)
    return merged
