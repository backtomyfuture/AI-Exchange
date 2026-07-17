"""
PDF generation + Lark Drive upload flow.

Extracted from ``lark_app`` so the giant module stops growing and these CPU /
network heavy paths can be tested in isolation.

Both functions accept their Lark dependencies as keyword arguments rather than
reaching into module-level globals. ``lark_app`` keeps thin shim functions for
backwards compatibility with callers that have not yet been migrated.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

from lark_oapi.api.im.v1 import (
    ReplyMessageRequest,
    ReplyMessageRequestBody,
)

from src.graph.dependencies import GraphDependencies
from src.graph.resource_locks import get_graph_resource_lock
from src.graph.state_factory import (
    MAX_TOKENS,
    hydrate_email_for_rendering,
    sanitize_graph_delta,
)
from src.security.redaction import fingerprint_identifier
from src.utils.email_renderer import render_email_html
from src.utils.pdf_generator import convert_html_to_pdf

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PdfFlowOutcome:
    """Explicit non-happy-path result for callers that need safe retry logic."""

    status: str
    retryable: bool = False
    reply_sent: bool = False
    cleanup_tokens: tuple[str, ...] = ()
    protected_tokens: tuple[str, ...] = ()


def _token_tuple(*tokens: object) -> tuple[str, ...]:
    result: list[str] = []
    for token in tokens:
        if isinstance(token, str) and token and token not in result:
            result.append(token)
    return tuple(result)


def _prepare_pdf_token_transition(
    values: dict[str, Any],
    target_token: str | None,
) -> tuple[dict[str, Any], dict[str, Any], str | None]:
    """Atomically switch the active PDF and register the replaced token."""
    previous_token = values.get("pdf_token")
    cleanup_tokens = list(values.get("attachment_tokens") or [])

    # An active PDF must never remain eligible for background cleanup.
    cleanup_tokens = [token for token in cleanup_tokens if token != target_token]
    replaced_token = (
        previous_token
        if isinstance(previous_token, str)
        and previous_token
        and previous_token != target_token
        else None
    )
    if replaced_token is not None and replaced_token not in cleanup_tokens:
        if len(cleanup_tokens) >= MAX_TOKENS:
            raise ValueError("cleanup_handle_capacity_exhausted")
        cleanup_tokens.append(replaced_token)

    delta: dict[str, Any] = {"pdf_token": target_token}
    if cleanup_tokens != list(values.get("attachment_tokens") or []):
        delta["attachment_tokens"] = cleanup_tokens
    elif replaced_token is not None:
        # Keep both fields in the same persisted write even when OLD had
        # already been registered by an earlier retry.
        delta["attachment_tokens"] = cleanup_tokens
    update = sanitize_graph_delta(values, delta)
    return update, {**values, **update}, replaced_token


def _pdf_transition_confirmed(
    values: Mapping[str, Any],
    *,
    active_token: str | None,
    cleanup_token: str | None,
) -> bool:
    cleanup_tokens = values.get("attachment_tokens") or []
    if values.get("pdf_token") != active_token or active_token in cleanup_tokens:
        return False
    return cleanup_token is None or cleanup_token in cleanup_tokens


async def _read_graph_values(
    graph: Any,
    config: dict[str, Any],
) -> tuple[str, dict[str, Any] | None]:
    """Read the latest state when supported, distinguishing absent from failed."""
    get_state = getattr(graph, "aget_state", None)
    if not callable(get_state):
        return "unavailable", None
    try:
        snapshot = await get_state(config)
        current = snapshot.values if hasattr(snapshot, "values") else snapshot
        if not isinstance(current, Mapping):
            return "failed", None
        return "loaded", dict(current)
    except Exception as exc:
        logger.error(
            "Graph state reconciliation failed: error_type=%s",
            type(exc).__name__,
        )
        return "failed", None


async def _retain_cleanup_handle(
    graph: Any,
    config: dict[str, Any],
    values: dict[str, Any],
    token: str,
) -> bool:
    read_status, current_values = await _read_graph_values(graph, config)
    if read_status == "failed":
        return False
    base_values = current_values if current_values is not None else values
    cleanup_tokens = list(base_values.get("attachment_tokens") or [])
    if token in cleanup_tokens:
        return True
    if len(cleanup_tokens) >= MAX_TOKENS:
        logger.error("Remote cleanup handle list is full")
        return False
    cleanup_tokens.append(token)
    try:
        cleanup_update = sanitize_graph_delta(
            base_values,
            {"attachment_tokens": cleanup_tokens},
        )
        await graph.aupdate_state(config, cleanup_update)
        return True
    except Exception as exc:
        logger.error(
            "Remote cleanup handle persistence failed: error_type=%s",
            type(exc).__name__,
        )
        read_status, current_values = await _read_graph_values(graph, config)
        if read_status == "loaded" and token in (
            current_values.get("attachment_tokens") or []
        ):
            return True
        return False


async def _delete_or_retain(
    graph: Any,
    config: dict[str, Any],
    values: dict[str, Any],
    token: str,
    delete_fn: Callable[[str], bool],
) -> str:
    try:
        deleted = await asyncio.to_thread(delete_fn, token)
    except Exception as exc:
        logger.error(
            "Drive cleanup failed: error_type=%s",
            type(exc).__name__,
        )
        deleted = False
    if deleted:
        return "deleted"
    retained = await _retain_cleanup_handle(graph, config, values, token)
    return "retained" if retained else "untracked"


async def _delete_registered_cleanup_handle(
    graph: Any,
    config: dict[str, Any],
    values: dict[str, Any],
    *,
    active_token: str | None,
    cleanup_token: str,
    delete_fn: Callable[[str], bool],
) -> str:
    """Delete only a token registered against the expected active PDF."""
    read_status, current_values = await _read_graph_values(graph, config)
    if read_status == "failed":
        return "protected"
    base_values = current_values if current_values is not None else values
    cleanup_tokens = list(base_values.get("attachment_tokens") or [])
    if base_values.get("pdf_token") != active_token:
        return "protected"
    if cleanup_token not in cleanup_tokens:
        return "untracked"

    try:
        deleted = await asyncio.to_thread(delete_fn, cleanup_token)
    except Exception as exc:
        logger.error(
            "Drive cleanup failed: error_type=%s",
            type(exc).__name__,
        )
        deleted = False
    if not deleted:
        return "retained"

    read_status, current_values = await _read_graph_values(graph, config)
    if read_status == "failed":
        return "stale"
    if read_status == "loaded":
        current_cleanup_tokens = list(
            current_values.get("attachment_tokens") or []
        )
        if cleanup_token not in current_cleanup_tokens:
            return "deleted"
        if current_values.get("pdf_token") != active_token:
            return "stale"
        base_values = current_values
        cleanup_tokens = current_cleanup_tokens

    remaining_tokens = [token for token in cleanup_tokens if token != cleanup_token]
    try:
        removal = sanitize_graph_delta(
            base_values,
            {"attachment_tokens": remaining_tokens},
        )
        await graph.aupdate_state(config, removal)
        return "deleted"
    except Exception as exc:
        logger.error(
            "Cleanup handle removal failed: error_type=%s",
            type(exc).__name__,
        )
        read_status, current_values = await _read_graph_values(graph, config)
        if read_status == "loaded" and cleanup_token not in (
            current_values.get("attachment_tokens") or []
        ):
            return "deleted"
        return "stale"


async def generate_and_upload_pdf(
    email_id: str,
    state: Any,
    *,
    dependencies: GraphDependencies,
    upload_fn: Callable[[str, bytes, int], Optional[Dict[str, Any]]],
    delete_fn: Callable[[str], bool] | None = None,
) -> Optional[Dict[str, Any]] | PdfFlowOutcome:
    """
    Render an email to PDF and upload it to Lark Drive.

    Args:
        email_id: Logical thread/email id (used in the file name only).
        state: Slim Graph state wrapper containing only strict references.
        upload_fn: Callable executing the Lark Drive upload. Signature must be
            ``(filename, content_bytes, size) -> {"url", "file_token"} | None``.
            Injected so tests can stub the Lark side effect.

    Returns:
        ``{"url": ..., "file_token": ...}`` on success. Ordinary generation
        failures remain ``None`` for compatibility; an inconclusive remote
        cleanup returns :class:`PdfFlowOutcome` with the exact handle that the
        caller must reconcile.
    """
    try:
        logger.info(
            "Starting PDF generation: email=%s",
            fingerprint_identifier(email_id, namespace="email"),
        )
        loop = asyncio.get_running_loop()
        values = state.values if hasattr(state, "values") else state
        email_data = await hydrate_email_for_rendering(values, dependencies)

        html_content = await loop.run_in_executor(None, render_email_html, email_data)
        if html_content:
            logger.info("HTML content for PDF generated, size=%d bytes", len(html_content))
        else:
            logger.warning("HTML content for PDF is empty.")

        try:
            pdf_bytes = await loop.run_in_executor(None, convert_html_to_pdf, html_content)
        except Exception as exc:
            logger.error(
                "PDF conversion failed: error_type=%s",
                type(exc).__name__,
            )
            return None

        if not pdf_bytes:
            logger.error("PDF generation returned empty bytes.")
            return None

        filename = f"Email_Export_{email_id}.pdf"
        logger.info("Uploading PDF: size=%d", len(pdf_bytes))

        try:
            upload_resp = await loop.run_in_executor(
                None, upload_fn, filename, pdf_bytes, len(pdf_bytes)
            )
        except Exception as exc:
            logger.error(
                "Lark Drive upload failed: error_type=%s",
                type(exc).__name__,
            )
            return None

        if not upload_resp:
            logger.error("PDF upload returned empty response.")
            return None

        url = upload_resp.get("url")
        file_token = upload_resp.get("file_token")
        valid_url = (
            isinstance(url, str)
            and bool(url)
            and len(url.encode("utf-8")) <= 2_048
        )
        valid_token = (
            isinstance(file_token, str)
            and bool(file_token)
            and len(file_token.encode("utf-8")) <= 512
        )
        if not valid_url or not valid_token:
            cleanup_tokens = _token_tuple(file_token)
            known_tokens = set(_token_tuple(values.get("pdf_token")))
            known_tokens.update(
                token
                for token in (values.get("attachment_tokens") or [])
                if isinstance(token, str) and token
            )
            if cleanup_tokens and cleanup_tokens[0] in known_tokens:
                logger.error(
                    "PDF upload returned invalid identifiers for a protected handle"
                )
                return PdfFlowOutcome(
                    status="upload_invalid_protected_token",
                    retryable=True,
                    protected_tokens=cleanup_tokens,
                )
            deleted = False
            if cleanup_tokens and delete_fn is not None:
                try:
                    deleted = bool(
                        await asyncio.to_thread(delete_fn, cleanup_tokens[0])
                    )
                except Exception as exc:
                    logger.error(
                        "Invalid PDF upload cleanup failed: error_type=%s",
                        type(exc).__name__,
                    )
            if cleanup_tokens and not deleted:
                logger.error(
                    "PDF upload returned invalid identifiers; cleanup is required"
                )
                return PdfFlowOutcome(
                    status="upload_invalid_cleanup_required",
                    retryable=True,
                    cleanup_tokens=cleanup_tokens,
                )
            logger.error("PDF upload returned invalid identifiers")
            return None

        return {"url": url, "file_token": file_token}
    except Exception as exc:
        logger.error(
            "PDF generation failed: error_type=%s",
            type(exc).__name__,
        )
        return None


async def _process_pdf_generation_and_reply_locked(
    email_id: str,
    state: Any,
    message_id: str,
    *,
    graph: Any,
    dependencies: GraphDependencies,
    lark_api_client: Any,
    upload_fn: Callable[[str, bytes, int], Optional[Dict[str, Any]]],
    delete_fn: Callable[[str], bool],
) -> PdfFlowOutcome | None:
    """
    Generate the PDF, persist its file_token in graph state, and reply with a
    Lark card containing the open-PDF button.

    Dependencies are injected so we can mock graph / Lark / upload in tests.
    Every state/cleanup ambiguity is returned as :class:`PdfFlowOutcome` so an
    upper durable retry layer can distinguish success, pending cleanup, and a
    protected handle that must not be deleted before reconciliation.
    """
    try:
        if not lark_api_client:
            logger.warning("Lark API client not configured; skipping PDF generation.")
            return

        result = await generate_and_upload_pdf(
            email_id,
            state,
            dependencies=dependencies,
            upload_fn=upload_fn,
            delete_fn=delete_fn,
        )
        if isinstance(result, PdfFlowOutcome):
            return result
        if not result:
            return

        file_url = result["url"]
        file_token = result["file_token"]

        config = {"configurable": {"thread_id": email_id}}
        values = dict(state.values)
        read_status, current_values = await _read_graph_values(graph, config)
        if read_status == "loaded" and (
            not current_values
            or current_values.get("email_id") != values.get("email_id")
        ):
            logger.error("Latest Graph state did not match the PDF flow")
            read_status = "failed"
            current_values = None
        if read_status == "failed":
            cleanup = await _delete_or_retain(
                graph,
                config,
                values,
                file_token,
                delete_fn,
            )
            suffix = {
                "deleted": "cleaned",
                "retained": "cleanup_pending",
                "untracked": "cleanup_untracked",
            }[cleanup]
            return PdfFlowOutcome(
                status=f"state_precondition_read_failed_{suffix}",
                retryable=True,
                cleanup_tokens=() if cleanup == "deleted" else (file_token,),
                protected_tokens=_token_tuple(values.get("pdf_token")),
            )
        if current_values is not None:
            values = current_values
        old_token = values.get("pdf_token")
        try:
            update, staged_values, registered_old_token = (
                _prepare_pdf_token_transition(values, file_token)
            )
        except Exception as exc:
            logger.error(
                "PDF token transition precondition failed: error_type=%s",
                type(exc).__name__,
            )
            cleanup = await _delete_or_retain(
                graph,
                config,
                values,
                file_token,
                delete_fn,
            )
            suffix = {
                "deleted": "cleaned",
                "retained": "cleanup_pending",
                "untracked": "cleanup_untracked",
            }[cleanup]
            return PdfFlowOutcome(
                status=f"state_precondition_failed_{suffix}",
                retryable=True,
                cleanup_tokens=() if cleanup == "deleted" else (file_token,),
                protected_tokens=_token_tuple(old_token),
            )

        try:
            await graph.aupdate_state(config, update)
            values = staged_values
        except Exception as exc:
            logger.error(
                "PDF token persistence failed: error_type=%s",
                type(exc).__name__,
            )
            read_status, current_values = await _read_graph_values(graph, config)
            if read_status == "loaded":
                current_token = current_values.get("pdf_token")
                if _pdf_transition_confirmed(
                    current_values,
                    active_token=file_token,
                    cleanup_token=registered_old_token,
                ):
                    values = current_values
                elif current_token != old_token:
                    return PdfFlowOutcome(
                        status="state_write_ambiguous",
                        retryable=True,
                        protected_tokens=_token_tuple(
                            old_token,
                            file_token,
                            current_token,
                        ),
                    )
                else:
                    cleanup = await _delete_or_retain(
                        graph,
                        config,
                        current_values,
                        file_token,
                        delete_fn,
                    )
                    suffix = {
                        "deleted": "cleaned",
                        "retained": "cleanup_pending",
                        "untracked": "cleanup_untracked",
                    }[cleanup]
                    return PdfFlowOutcome(
                        status=f"state_write_failed_{suffix}",
                        retryable=True,
                        cleanup_tokens=(
                            () if cleanup == "deleted" else (file_token,)
                        ),
                        protected_tokens=_token_tuple(old_token),
                    )
            elif read_status == "failed":
                return PdfFlowOutcome(
                    status="state_write_ambiguous",
                    retryable=True,
                    protected_tokens=_token_tuple(old_token, file_token),
                )
            else:
                cleanup = await _delete_or_retain(
                    graph,
                    config,
                    values,
                    file_token,
                    delete_fn,
                )
                suffix = {
                    "deleted": "cleaned",
                    "retained": "cleanup_pending",
                    "untracked": "cleanup_untracked",
                }[cleanup]
                return PdfFlowOutcome(
                    status=f"state_write_failed_{suffix}",
                    retryable=True,
                    cleanup_tokens=() if cleanup == "deleted" else (file_token,),
                    protected_tokens=_token_tuple(old_token),
                )

        filename = f"Email_Export_{email_id}.pdf"
        card_content = {
            "header": {
                "template": "blue",
                "title": {"content": "📄 PDF 原文已生成", "tag": "plain_text"},
            },
            "elements": [
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": f"点击下方按钮查看 PDF 文件：\nFilename: *{filename}*",
                    },
                },
                {
                    "tag": "action",
                    "actions": [
                        {
                            "tag": "button",
                            "text": {"tag": "plain_text", "content": "📂 打开 PDF"},
                            "type": "primary",
                            "url": file_url,
                        }
                    ],
                },
            ],
        }

        req_msg = (
            ReplyMessageRequest.builder()
            .message_id(message_id)
            .request_body(
                ReplyMessageRequestBody.builder()
                .msg_type("interactive")
                .content(json.dumps(card_content))
                .build()
            )
            .build()
        )
        try:
            response = lark_api_client.im.v1.message.reply(req_msg)
            if hasattr(response, "success") and not response.success():
                raise RuntimeError("lark_pdf_reply_failed")
        except Exception as exc:
            logger.error(
                "PDF reply failed: error_type=%s",
                type(exc).__name__,
            )
            restore_already_done = False
            read_status, current_values = await _read_graph_values(graph, config)
            if read_status == "failed":
                return PdfFlowOutcome(
                    status="reply_failed_restore_ambiguous",
                    retryable=True,
                    protected_tokens=_token_tuple(old_token, file_token),
                )
            if read_status == "loaded":
                if _pdf_transition_confirmed(
                    current_values,
                    active_token=file_token,
                    cleanup_token=registered_old_token,
                ):
                    values = current_values
                elif _pdf_transition_confirmed(
                    current_values,
                    active_token=old_token,
                    cleanup_token=file_token,
                ):
                    values = current_values
                    restore_already_done = True
                else:
                    return PdfFlowOutcome(
                        status="reply_failed_restore_ambiguous",
                        retryable=True,
                        protected_tokens=_token_tuple(old_token, file_token),
                    )

            if not restore_already_done:
                try:
                    restore, restored_values, registered_new_token = (
                        _prepare_pdf_token_transition(values, old_token)
                    )
                except Exception as restore_exc:
                    logger.error(
                        "PDF token restore precondition failed: error_type=%s",
                        type(restore_exc).__name__,
                    )
                    return PdfFlowOutcome(
                        status="reply_failed_restore_precondition_failed",
                        retryable=True,
                        protected_tokens=_token_tuple(old_token, file_token),
                    )
                try:
                    await graph.aupdate_state(config, restore)
                    values = restored_values
                except Exception as restore_exc:
                    logger.error(
                        "PDF token restore failed: error_type=%s",
                        type(restore_exc).__name__,
                    )
                    read_status, current_values = await _read_graph_values(
                        graph,
                        config,
                    )
                    if read_status == "loaded" and _pdf_transition_confirmed(
                        current_values,
                        active_token=old_token,
                        cleanup_token=registered_new_token,
                    ):
                        values = current_values
                    elif read_status == "failed":
                        return PdfFlowOutcome(
                            status="reply_failed_restore_ambiguous",
                            retryable=True,
                            protected_tokens=_token_tuple(old_token, file_token),
                        )
                    else:
                        current_token = (
                            current_values.get("pdf_token")
                            if current_values is not None
                            else None
                        )
                        return PdfFlowOutcome(
                            status="reply_failed_restore_failed",
                            retryable=True,
                            protected_tokens=_token_tuple(
                                old_token,
                                file_token,
                                current_token,
                            ),
                        )
            else:
                registered_new_token = (
                    file_token if file_token != old_token else None
                )

            cleanup = "deleted"
            if registered_new_token is not None:
                cleanup = await _delete_registered_cleanup_handle(
                    graph,
                    config,
                    values,
                    active_token=old_token,
                    cleanup_token=registered_new_token,
                    delete_fn=delete_fn,
                )
            suffix = {
                "deleted": "rolled_back",
                "retained": "cleanup_pending",
                "untracked": "cleanup_untracked",
                "protected": "cleanup_protected",
                "stale": "cleanup_stale",
            }[cleanup]
            return PdfFlowOutcome(
                status=f"reply_failed_{suffix}",
                retryable=True,
                cleanup_tokens=(
                    ()
                    if cleanup in {"deleted", "protected"}
                    else _token_tuple(registered_new_token)
                ),
                protected_tokens=(
                    _token_tuple(old_token, file_token)
                    if cleanup == "protected"
                    else ()
                ),
            )

        if registered_old_token is not None:
            cleanup = await _delete_registered_cleanup_handle(
                graph,
                config,
                values,
                active_token=file_token,
                cleanup_token=registered_old_token,
                delete_fn=delete_fn,
            )
            if cleanup != "deleted":
                suffix = {
                    "retained": "cleanup_pending",
                    "untracked": "cleanup_untracked",
                    "protected": "cleanup_protected",
                    "stale": "cleanup_stale",
                }[cleanup]
                logger.warning(
                    "PDF reply sent but replaced file cleanup is pending: status=%s",
                    suffix,
                )
                return PdfFlowOutcome(
                    status=f"reply_sent_{suffix}",
                    retryable=True,
                    reply_sent=True,
                    cleanup_tokens=(
                        ()
                        if cleanup == "protected"
                        else (registered_old_token,)
                    ),
                    protected_tokens=(
                        _token_tuple(old_token, file_token)
                        if cleanup == "protected"
                        else ()
                    ),
                )
        logger.info(
            "PDF reply sent successfully: email=%s",
            fingerprint_identifier(email_id, namespace="email"),
        )
        return PdfFlowOutcome(status="reply_sent", reply_sent=True)
    except Exception as exc:
        logger.error(
            "PDF reply flow failed: error_type=%s",
            type(exc).__name__,
        )
        return PdfFlowOutcome(status="reply_flow_failed", retryable=True)


async def process_pdf_generation_and_reply(
    email_id: str,
    state: Any,
    message_id: str,
    *,
    graph: Any,
    dependencies: GraphDependencies,
    lark_api_client: Any,
    upload_fn: Callable[[str, bytes, int], Optional[Dict[str, Any]]],
    delete_fn: Callable[[str], bool],
) -> PdfFlowOutcome | None:
    """Serialize PDF token transitions for one email inside this worker."""
    async with get_graph_resource_lock(email_id):
        return await _process_pdf_generation_and_reply_locked(
            email_id,
            state,
            message_id,
            graph=graph,
            dependencies=dependencies,
            lark_api_client=lark_api_client,
            upload_fn=upload_fn,
            delete_fn=delete_fn,
        )
