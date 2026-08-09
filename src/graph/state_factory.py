from __future__ import annotations

import json
from copy import deepcopy
from collections.abc import Mapping, Sequence
from itertools import islice
from typing import Any

from src.config import get_settings
from src.graph.dependencies import GraphDependencies
from src.storage import ContentRef, ContentStoreReferenceError


MAX_CHECKPOINT_BYTES = 16_384
# LangGraph persists channel versions, updated-channel metadata and checkpoint
# envelopes in addition to the logical State.  Keep a fixed 4 KiB reserve so
# every real serializer unit remains strictly below the 16 KiB hard limit.
CHECKPOINT_WRAPPER_HEADROOM_BYTES = 4_096
MAX_LOGICAL_STATE_BYTES = MAX_CHECKPOINT_BYTES - CHECKPOINT_WRAPPER_HEADROOM_BYTES
MAX_SUBJECT_BYTES = 768
MAX_SENDER_BYTES = 320
MAX_ID_BYTES = 512
MAX_DATE_BYTES = 128
MAX_RECIPIENTS = 10
MAX_RECIPIENT_BYTES = 320
MAX_CLASSIFICATION_TEXT_BYTES = 512
MAX_CONTEXT_SUMMARIES = 5
MAX_CONTEXT_SNIPPET_BYTES = 384
MAX_ROUTING_LOGS = 8
MAX_ROUTING_LOG_BYTES = 256
_ROUTING_STAGES = frozenset({"pending", "tier1", "tier2", "tier3", "none"})
MAX_METADATA_TEXT_BYTES = 1_024
# The visual summary is drafting context, not a transcript.  Keep enough room
# for the rest of the LangGraph checkpoint (routing, context and review state).
MAX_IMAGE_ANALYSIS_BYTES = 1_024
MAX_REVIEW_TEXT_BYTES = 512
MAX_TOKENS = 32

_REF_FIELDS = frozenset({"account_id", "object_id", "key_version", "sha256"})
_EMAIL_METADATA_CAPS = {
    "subject": MAX_SUBJECT_BYTES,
    "sender": MAX_SENDER_BYTES,
    "thread_id": MAX_ID_BYTES,
    "conversation_id": MAX_ID_BYTES,
    "received_at": MAX_DATE_BYTES,
}
_CLASSIFICATION_FIELDS = frozenset(
    {
        "priority",
        "need_reply",
        "intent",
        "summary",
        "reasoning",
        "confidence",
        "action",
        "target_recipient",
    }
)
_METADATA_FIELDS = frozenset(
    {
        "review_count",
        "review_issues",
        "thread_summary",
        "style_guidance",
        "image_analysis",
        "experience_hints",
        "preference_hints",
        "content_guard",
    }
)
_FORBIDDEN_DELTA_FIELDS = frozenset(
    {"email", "draft", "feedback", "context", "reply_examples"}
)


def truncate_utf8(value: object, *, max_bytes: int) -> str:
    if max_bytes < 0:
        raise ValueError("invalid_byte_limit")
    text = value if isinstance(value, str) else str(value or "")
    encoded = text.encode("utf-8")
    if len(encoded) <= max_bytes:
        return text
    return encoded[:max_bytes].decode("utf-8", errors="ignore")


def cap_string_list(
    values: object,
    *,
    max_items: int,
    max_item_bytes: int,
) -> list[str]:
    if not isinstance(values, Sequence) or isinstance(values, (str, bytes, bytearray)):
        return []
    return [
        truncate_utf8(value, max_bytes=max_item_bytes)
        for value in islice(iter(values), max_items)
    ]


def cap_identifier_list(
    values: object,
    *,
    field: str,
    max_items: int,
    max_item_bytes: int,
    reject_excess: bool = False,
) -> list[str]:
    if not isinstance(values, Sequence) or isinstance(values, (str, bytes, bytearray)):
        raise ValueError(f"invalid_{field}")
    if reject_excess and len(values) > max_items:
        raise ValueError(f"invalid_{field}")
    result: list[str] = []
    for value in islice(iter(values), max_items):
        if (
            not isinstance(value, str)
            or not value
            or len(value.encode("utf-8")) > max_item_bytes
        ):
            raise ValueError(f"invalid_{field}")
        result.append(value)
    return result


def content_ref_to_json(ref: ContentRef) -> dict[str, Any]:
    if not isinstance(ref, ContentRef):
        raise ContentStoreReferenceError("invalid_content_ref")
    validated = ContentRef(
        account_id=ref.account_id,
        object_id=ref.object_id,
        key_version=ref.key_version,
        sha256=ref.sha256,
    )
    return {
        "account_id": validated.account_id,
        "object_id": validated.object_id,
        "key_version": validated.key_version,
        "sha256": validated.sha256,
    }


def content_ref_from_json(
    value: object,
    *,
    expected_account_id: int | None = None,
) -> ContentRef:
    if not isinstance(value, Mapping) or set(value) != _REF_FIELDS:
        raise ContentStoreReferenceError("invalid_content_ref")
    try:
        ref = ContentRef(
            account_id=value["account_id"],
            object_id=value["object_id"],
            key_version=value["key_version"],
            sha256=value["sha256"],
        )
    except (KeyError, TypeError):
        raise ContentStoreReferenceError("invalid_content_ref") from None
    if expected_account_id is not None and ref.account_id != expected_account_id:
        raise ContentStoreReferenceError("content_account_mismatch")
    return ref


def require_owned_content_ref(
    ref: object,
    *,
    expected_account_id: int,
) -> ContentRef:
    if not isinstance(ref, ContentRef):
        raise ContentStoreReferenceError("missing_content_ref")
    if ref.account_id != expected_account_id:
        raise ContentStoreReferenceError("content_account_mismatch")
    return ref


def serialized_state_size(value: Mapping[str, Any]) -> int:
    try:
        return len(
            json.dumps(
                value,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8")
        )
    except (TypeError, ValueError, UnicodeEncodeError):
        raise ValueError("graph_state_not_serializable") from None


def ensure_state_size(value: Mapping[str, Any]) -> None:
    if serialized_state_size(value) >= MAX_LOGICAL_STATE_BYTES:
        raise ValueError("graph_state_too_large")


def _bounded_identifier(value: object, *, field: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value.encode("utf-8")) > MAX_ID_BYTES
    ):
        raise ValueError(f"invalid_{field}")
    return value


def validate_initial_graph_metadata(
    metadata: Mapping[str, Any],
    *,
    reject_recipient_excess: bool = False,
) -> dict[str, list[str]]:
    """Validate identifiers that would otherwise fail after durable writes."""
    if not isinstance(metadata, Mapping):
        raise ValueError("invalid_email_metadata")
    _bounded_identifier(metadata.get("id"), field="email_id")

    raw_to = (
        metadata.get("draft_to")
        if "draft_to" in metadata
        else ([metadata.get("sender")] if metadata.get("sender") else [])
    )
    raw_cc = (
        metadata.get("draft_cc")
        if "draft_cc" in metadata
        else metadata.get("cc", [])
    )
    return {
        "draft_to": cap_identifier_list(
            raw_to,
            field="draft_to",
            max_items=MAX_RECIPIENTS,
            max_item_bytes=MAX_RECIPIENT_BYTES,
            reject_excess=reject_recipient_excess,
        ),
        "draft_cc": cap_identifier_list(
            raw_cc,
            field="draft_cc",
            max_items=MAX_RECIPIENTS,
            max_item_bytes=MAX_RECIPIENT_BYTES,
            reject_excess=reject_recipient_excess,
        ),
    }


def require_owned_draft_id(
    state: Mapping[str, Any],
    draft_id: object,
) -> str:
    email_id = _bounded_identifier(state.get("email_id"), field="email_id")
    owned_draft_id = _bounded_identifier(draft_id, field="draft_id")
    if owned_draft_id != email_id:
        raise ValueError("draft_email_mismatch")
    return owned_draft_id


def _sanitize_classification(value: object) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    result: dict[str, Any] = {}
    for key in _CLASSIFICATION_FIELDS:
        if key not in value:
            continue
        item = value[key]
        if key in {"need_reply"}:
            if type(item) is bool:
                result[key] = item
        elif key == "confidence":
            if isinstance(item, (int, float)) and not isinstance(item, bool):
                result[key] = max(0.0, min(1.0, float(item)))
        else:
            result[key] = truncate_utf8(
                item,
                max_bytes=MAX_CLASSIFICATION_TEXT_BYTES,
            )
    return result


def _sanitize_context_summaries(value: object) -> list[dict[str, str]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    summaries: list[dict[str, str]] = []
    for raw in islice(iter(value), MAX_CONTEXT_SUMMARIES):
        if not isinstance(raw, Mapping):
            continue
        summary: dict[str, str] = {}
        for field, limit in {
            "id": MAX_ID_BYTES,
            "sender": MAX_SENDER_BYTES,
            "subject": MAX_SUBJECT_BYTES,
            "snippet": MAX_CONTEXT_SNIPPET_BYTES,
        }.items():
            if raw.get(field) not in (None, ""):
                summary[field] = truncate_utf8(raw[field], max_bytes=limit)
        summaries.append(summary)
    return summaries


def _sanitize_hint_list(
    value: object,
    *,
    max_items: int,
) -> list[dict[str, Any]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    result: list[dict[str, Any]] = []
    for raw in islice(iter(value), max_items):
        if not isinstance(raw, Mapping):
            continue
        item: dict[str, Any] = {}
        for field in ("category", "pattern"):
            if raw.get(field) not in (None, ""):
                item[field] = truncate_utf8(
                    raw[field],
                    max_bytes=64 if field == "category" else 384,
                )
        confidence = raw.get("confidence")
        if isinstance(confidence, (int, float)) and not isinstance(confidence, bool):
            item["confidence"] = max(0.0, min(1.0, float(confidence)))
        if item:
            result.append(item)
    return result


def _sanitize_content_guard(value: object) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    result: dict[str, Any] = {}
    if type(value.get("passed")) is bool:
        result["passed"] = value["passed"]
    if value.get("summary") not in (None, ""):
        result["summary"] = truncate_utf8(
            value["summary"], max_bytes=MAX_REVIEW_TEXT_BYTES
        )
    for field in ("sensitive_issues", "hallucination_issues"):
        if field in value:
            result[field] = cap_string_list(
                value[field], max_items=5, max_item_bytes=256
            )
    return result


def _sanitize_metadata(current: object, value: object) -> dict[str, Any]:
    source: dict[str, Any] = {}
    if isinstance(current, Mapping):
        source.update({k: v for k, v in current.items() if k in _METADATA_FIELDS})
    if isinstance(value, Mapping):
        source.update({k: v for k, v in value.items() if k in _METADATA_FIELDS})

    result: dict[str, Any] = {}
    review_count = source.get("review_count")
    if type(review_count) is int:
        result["review_count"] = max(0, min(review_count, 10))
    for field, limit in {
        "review_issues": MAX_REVIEW_TEXT_BYTES,
        "thread_summary": MAX_METADATA_TEXT_BYTES,
        "style_guidance": MAX_METADATA_TEXT_BYTES,
        "image_analysis": MAX_IMAGE_ANALYSIS_BYTES,
    }.items():
        if source.get(field) is not None:
            result[field] = truncate_utf8(source[field], max_bytes=limit)
    if "experience_hints" in source:
        result["experience_hints"] = _sanitize_hint_list(
            source["experience_hints"], max_items=3
        )
    if "preference_hints" in source:
        result["preference_hints"] = _sanitize_hint_list(
            source["preference_hints"], max_items=5
        )
    if "content_guard" in source:
        result["content_guard"] = _sanitize_content_guard(source["content_guard"])
    return result


def _sanitize_review_result(value: object) -> dict[str, Any] | None:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        return None
    result: dict[str, Any] = {}
    if type(value.get("passed")) is bool:
        result["passed"] = value["passed"]
    for field in ("summary", "issues"):
        if value.get(field) is not None:
            result[field] = truncate_utf8(
                value[field], max_bytes=MAX_REVIEW_TEXT_BYTES
            )
    return result


def _sanitize_route_decision(value: object) -> dict[str, Any]:
    from src.router.decision import RouteDecision

    try:
        decision = RouteDecision.model_validate(value)
    except Exception:
        raise ValueError("invalid_route_decision") from None
    return decision.model_dump(mode="json")


def _sanitize_tool_calls(value: object) -> list[dict[str, str]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    result: list[dict[str, str]] = []
    for raw in islice(iter(value), 8):
        if not isinstance(raw, Mapping):
            continue
        item = {
            field: truncate_utf8(raw[field], max_bytes=256)
            for field in ("id", "name", "status", "result_summary")
            if raw.get(field) not in (None, "")
        }
        if item:
            result.append(item)
    return result


def _sanitize_recipient_ui(
    current: object,
    value: object,
) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    if isinstance(current, Mapping):
        for field in ("to", "cc"):
            existing = current.get(field)
            if isinstance(existing, Mapping):
                result[field] = dict(existing)
    if not isinstance(value, Mapping):
        return result

    for field in ("to", "cc"):
        if field not in value:
            continue
        raw = value[field]
        if not isinstance(raw, Mapping) or not raw:
            result.pop(field, None)
            continue
        ui: dict[str, Any] = {}
        if "options" in raw:
            ui["options"] = cap_identifier_list(
                raw["options"],
                field="recipient_ui_options",
                max_items=20,
                max_item_bytes=128,
            )
        if "selected" in raw:
            ui["selected"] = cap_identifier_list(
                raw["selected"],
                field="recipient_ui_selected",
                max_items=MAX_RECIPIENTS,
                max_item_bytes=128,
            )
        if raw.get("external_input") is not None:
            ui["external_input"] = truncate_utf8(
                raw["external_input"], max_bytes=512
            )
        if raw.get("search_hint") is not None:
            ui["search_hint"] = truncate_utf8(raw["search_hint"], max_bytes=256)
        if ui:
            result[field] = ui
        else:
            result.pop(field, None)
    return result


def sanitize_graph_delta(
    state: Mapping[str, Any],
    delta: Mapping[str, Any],
) -> dict[str, Any]:
    """Allowlist and byte-cap node output before LangGraph persists it."""
    if not isinstance(state, Mapping) or not isinstance(delta, Mapping):
        raise ValueError("invalid_graph_delta")
    if _FORBIDDEN_DELTA_FIELDS.intersection(delta):
        # Forbidden payload fields are intentionally discarded below.
        pass

    result: dict[str, Any] = {}
    if "classification" in delta:
        result["classification"] = _sanitize_classification(delta["classification"])
    if "context_summaries" in delta:
        result["context_summaries"] = _sanitize_context_summaries(
            delta["context_summaries"]
        )
    if "draft_id" in delta:
        result["draft_id"] = (
            None
            if delta["draft_id"] is None
            else require_owned_draft_id(state, delta["draft_id"])
        )
    for field in ("draft_to", "draft_cc"):
        if field in delta:
            result[field] = cap_identifier_list(
                delta[field],
                field=field,
                max_items=MAX_RECIPIENTS,
                max_item_bytes=MAX_RECIPIENT_BYTES,
                reject_excess=True,
            )
    if "routing_log" in delta:
        result["routing_log"] = cap_string_list(
            delta["routing_log"],
            max_items=MAX_ROUTING_LOGS,
            max_item_bytes=MAX_ROUTING_LOG_BYTES,
        )
    if "routing_stage" in delta:
        stage = delta["routing_stage"]
        if type(stage) is not str or stage not in _ROUTING_STAGES:
            raise ValueError("invalid_routing_stage")
        result["routing_stage"] = stage
    if "route_decision" in delta:
        result["route_decision"] = _sanitize_route_decision(delta["route_decision"])
    if "priority_level" in delta and type(delta["priority_level"]) is int:
        result["priority_level"] = max(0, min(delta["priority_level"], 10))
    if "system_prompt_modifier" in delta:
        result["system_prompt_modifier"] = (
            None
            if delta["system_prompt_modifier"] is None
            else truncate_utf8(delta["system_prompt_modifier"], max_bytes=1_024)
        )
    if "tool_calls" in delta:
        result["tool_calls"] = _sanitize_tool_calls(delta["tool_calls"])
    for field in ("approval_status", "next_step"):
        if field in delta:
            result[field] = truncate_utf8(delta[field], max_bytes=64)
    for field in ("pdf_token",):
        if field in delta:
            result[field] = (
                None
                if delta[field] is None
                else _bounded_identifier(delta[field], field=field)
            )
    if "attachment_tokens" in delta:
        tokens = cap_identifier_list(
            delta["attachment_tokens"],
            field="attachment_token",
            max_items=MAX_TOKENS,
            max_item_bytes=MAX_ID_BYTES,
            reject_excess=True,
        )
        result["attachment_tokens"] = tokens
    if "metadata" in delta:
        result["metadata"] = _sanitize_metadata(state.get("metadata"), delta["metadata"])
    if "review_result" in delta:
        result["review_result"] = _sanitize_review_result(delta["review_result"])
    if "safe_error_summary" in delta:
        result["safe_error_summary"] = (
            None
            if delta["safe_error_summary"] is None
            else truncate_utf8(delta["safe_error_summary"], max_bytes=256)
        )
    if "recipient_ui" in delta:
        result["recipient_ui"] = _sanitize_recipient_ui(
            state.get("recipient_ui"),
            delta["recipient_ui"],
        )

    ensure_state_size({**state, **result})
    return result


async def hydrate_email_from_state(
    state: Mapping[str, Any],
    dependencies: GraphDependencies,
) -> dict[str, Any]:
    """Load the complete email into node-local memory after ownership validation."""
    return await _hydrate_email_from_state(
        state,
        dependencies,
        include_attachments=False,
    )


async def hydrate_email_for_rendering(
    state: Mapping[str, Any],
    dependencies: GraphDependencies,
) -> dict[str, Any]:
    """Load attachment bytes only for an immediate trusted edge renderer."""
    return await _hydrate_email_from_state(
        state,
        dependencies,
        include_attachments=True,
    )


async def hydrate_email_for_image_analysis(
    state: Mapping[str, Any],
    dependencies: GraphDependencies,
) -> dict[str, Any]:
    """Load attachment bytes only for the immediate visual-summary boundary."""
    return await _hydrate_email_from_state(
        state,
        dependencies,
        include_attachments=True,
    )


async def _hydrate_email_from_state(
    state: Mapping[str, Any],
    dependencies: GraphDependencies,
    *,
    include_attachments: bool,
) -> dict[str, Any]:
    email_id = _bounded_identifier(state.get("email_id"), field="email_id")
    expected_account_id = get_settings().EXCHANGE_ACCOUNT_ID
    ref = content_ref_from_json(
        state.get("content_ref"),
        expected_account_id=expected_account_id,
    )
    loaded = await dependencies.content_store.load_email(
        ref,
        include_attachments=include_attachments,
    )
    if not isinstance(loaded, Mapping):
        raise ValueError("invalid_hydrated_email")
    loaded_id = loaded.get("id")
    if loaded_id not in (None, "", email_id):
        raise ValueError("content_email_mismatch")

    email = deepcopy(dict(loaded))
    email["id"] = email_id
    slim_email = state.get("email")
    if isinstance(slim_email, Mapping):
        for field in _EMAIL_METADATA_CAPS:
            if field not in email and slim_email.get(field) not in (None, ""):
                email[field] = slim_email[field]
    email["draft_to"] = list(state.get("draft_to") or [])
    email["draft_cc"] = list(state.get("draft_cc") or [])

    attachments = email.get("attachments")
    if not include_attachments and isinstance(attachments, list):
        for attachment in attachments:
            if isinstance(attachment, dict):
                attachment.pop("content", None)
    email.pop("_image_attachments", None)
    return email


async def hydrate_draft_from_state(
    state: Mapping[str, Any],
    dependencies: GraphDependencies,
) -> str:
    """Resolve a full draft through its bounded stable identifier."""
    draft_id = require_owned_draft_id(state, state.get("draft_id"))
    draft = await dependencies.drafts.load_draft(draft_id)
    if not isinstance(draft, str):
        raise ValueError("invalid_hydrated_draft")
    return draft


async def hydrate_graph_content(
    state: Mapping[str, Any],
    dependencies: GraphDependencies,
    *,
    require_draft: bool = True,
) -> tuple[dict[str, Any], str]:
    """Shared production edge resolver for strict content_ref + draft_id State."""
    email = await hydrate_email_from_state(state, dependencies)
    if state.get("draft_id") is None and not require_draft:
        draft = ""
    else:
        draft = await hydrate_draft_from_state(state, dependencies)
    return email, draft


def build_initial_graph_state(
    metadata: Mapping[str, Any],
    ref: ContentRef,
) -> dict[str, Any]:
    recipients = validate_initial_graph_metadata(metadata)
    email_id = metadata["id"]

    email_metadata = {
        key: truncate_utf8(metadata.get(key, ""), max_bytes=max_bytes)
        for key, max_bytes in _EMAIL_METADATA_CAPS.items()
        if metadata.get(key) not in (None, "")
    }
    state: dict[str, Any] = {
        "email_id": email_id,
        "email": email_metadata,
        "content_ref": content_ref_to_json(ref),
        "classification": {},
        "context_summaries": [],
        "draft_id": None,
        "draft_to": recipients["draft_to"],
        "draft_cc": recipients["draft_cc"],
        "routing_log": [],
        "priority_level": 0,
        "system_prompt_modifier": None,
        "tool_calls": [],
        "approval_status": "pending",
        "next_step": "",
        "pdf_token": None,
        "attachment_tokens": [],
        "metadata": {},
        "review_result": None,
        "safe_error_summary": None,
        "recipient_ui": {},
    }
    ensure_state_size(state)
    return state
