from __future__ import annotations

import re


def normalize_recipient_address(value: object) -> str | None:
    """Return one bounded mailbox address, rejecting ambiguous input."""
    if not isinstance(value, str):
        return None
    candidate = value.strip()
    if (
        not candidate
        or len(candidate.encode("utf-8")) > 320
        or any(ord(character) < 32 or ord(character) == 127 for character in candidate)
        or any(character in candidate for character in (",", ";"))
    ):
        return None

    if "<" in candidate or ">" in candidate:
        display_match = re.fullmatch(r"[^<>]*<([^<>]+)>", candidate)
        if not display_match:
            return None
        address = display_match.group(1).strip()
    else:
        address = candidate

    if (
        address.count("@") != 1
        or any(character.isspace() for character in address)
        or any(character in address for character in "()<>[],:;\\\"")
    ):
        return None
    local_part, domain = address.rsplit("@", 1)
    if (
        not local_part
        or not domain
        or len(local_part.encode("utf-8")) > 64
        or len(domain.encode("utf-8")) > 253
        or local_part.startswith(".")
        or local_part.endswith(".")
        or domain.startswith(".")
        or domain.endswith(".")
        or ".." in local_part
        or ".." in domain
    ):
        return None
    allowed_local_punctuation = frozenset(".!#$%&'*+/=?^_`{|}~-")
    if any(
        not character.isalnum() and character not in allowed_local_punctuation
        for character in local_part
    ):
        return None
    for label in domain.split("."):
        if (
            not label
            or len(label.encode("utf-8")) > 63
            or label.startswith("-")
            or label.endswith("-")
            or any(
                not character.isalnum() and character != "-"
                for character in label
            )
        ):
            return None
    return address
