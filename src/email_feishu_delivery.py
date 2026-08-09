"""Outbound Email Feishu Delivery for one Inbound Email.

The public seam is :meth:`EmailFeishuDelivery.deliver`. It owns the card,
Review Material PDF, Business Attachment handles, and delivery-specific status.
Exchange mark-as-read and Inbox completion remain in the email orchestration.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
from collections.abc import Awaitable, Callable, Mapping
from copy import deepcopy
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from src.config import get_settings
from src.graph.dependencies import GraphDependencies
from src.graph.resource_locks import get_graph_resource_lock
from src.graph.state_factory import MAX_TOKENS, sanitize_graph_delta
from src.ingestion.processing import ExternalEffectBoundary, ExternalEffectKind
from src.router.decision import RouteDecision
from src.safety.attachments import AttachmentPolicy
from src.safety.input_limits import input_limits_from_settings
from src.safety.manual_review import normalize_manual_review_code
from src.safety.recipients import (
    ResolvedRecipients,
    recipients_follow_route,
)
from src.utils.email_attachments import select_business_attachments
from src.utils.lark_pdf_flow import PdfFlowOutcome


logger = logging.getLogger(__name__)

MANUAL_REVIEW_SOURCE_STATUSES = frozenset(
    {
        "pending",
        "recovering",
        "ingested",
        "analyzed",
        "drafted",
        "error",
        "delivery_failed",
    }
)
DELIVERY_OUTCOME_UNKNOWN_CODE = "feishu_delivery_outcome_unknown"


class EmailDeliveryKind(StrEnum):
    APPROVAL = "approval"
    MANUAL_REVIEW = "manual_review"
    READ_NOTIFICATION = "read_only"


class EmailDeliveryDisposition(StrEnum):
    CONFIRMED = "confirmed"
    KNOWN_FAILURE = "known_failure"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class LarkCardDelivery:
    """The observable Lark result of creating one interactive email card."""

    accepted: bool
    outcome_known: bool
    message_id: str | None = None


@dataclass(frozen=True, slots=True)
class EmailDeliveryOutcome:
    """The durable delivery fact returned to the email-processing orchestration."""

    kind: EmailDeliveryKind
    disposition: EmailDeliveryDisposition
    pdf_token: str | None = None
    message_id: str | None = None


class EmailDeliverySideEffectCommittedError(RuntimeError):
    """A card was accepted before local state could be made durable.

    The caller must preserve the current Delivery Resources and let the durable
    Inbox recovery path resolve the local state instead of sending another card.
    """

    def __init__(self, *, kind: EmailDeliveryKind, cause: BaseException) -> None:
        super().__init__("email_delivery_side_effect_committed")
        self.kind = kind
        self.cause = cause


@dataclass(frozen=True, slots=True)
class ReadNotificationRequest:
    """Everything needed to deliver one read-notification card."""

    email_id: str
    email_data: Mapping[str, Any]
    classification: Mapping[str, Any]
    context: tuple[Mapping[str, Any], ...]
    routing_log: tuple[object, ...]


@dataclass(frozen=True, slots=True)
class ApprovalRequest:
    """Everything needed to deliver one draft-approval card."""

    email_id: str
    email_data: Mapping[str, Any]
    classification: Mapping[str, Any]
    draft: str
    context: tuple[Mapping[str, Any], ...]
    routing_log: tuple[object, ...]
    inbox_id: str | None = None
    payload_revision: int | None = None
    payload_digest: str | None = None


@dataclass(frozen=True, slots=True)
class ManualReviewNotificationRequest:
    """Everything needed to deliver one manual-review card."""

    email_id: str
    email_data: Mapping[str, Any]
    classification: Mapping[str, Any]
    reason: str
    context: tuple[Mapping[str, Any], ...]
    routing_log: tuple[object, ...]


EmailDeliveryRequest = (
    ReadNotificationRequest | ApprovalRequest | ManualReviewNotificationRequest
)
PdfGenerator = Callable[..., Awaitable[object]]
CardSender = Callable[[EmailDeliveryRequest, str], LarkCardDelivery | bool]
DriveUpload = Callable[[str, bytes, int], object]
DriveDelete = Callable[[str], bool]
RecipientResolver = Callable[[object, object], Awaitable[ResolvedRecipients | None]]


@dataclass(frozen=True, slots=True)
class _PdfStage:
    ready: bool
    url: str | None = None
    old_token: str | None = None
    new_token: str | None = None
    error_code: str | None = None


class EmailFeishuDelivery:
    """Deep module for one typed Email Feishu Delivery request."""

    def __init__(
        self,
        *,
        database: Any,
        graph: Any,
        graph_dependencies: GraphDependencies,
        generate_pdf: PdfGenerator,
        send_card: CardSender,
        upload_file: DriveUpload,
        delete_file: DriveDelete,
        resolve_approval_recipients: RecipientResolver | None = None,
    ) -> None:
        self._database = database
        self._graph = graph
        self._graph_dependencies = graph_dependencies
        self._generate_pdf = generate_pdf
        self._send_card = send_card
        self._upload_file = upload_file
        self._delete_file = delete_file
        self._resolve_approval_recipients = resolve_approval_recipients

    async def deliver(
        self,
        request: EmailDeliveryRequest,
        effect_boundary: ExternalEffectBoundary | None,
    ) -> EmailDeliveryOutcome:
        """Deliver one typed card and persist its delivery-specific state.

        A card request with an indeterminate network outcome is deliberately
        quarantined as manual review. This is the only public operation of the
        module; callers do not receive raw PDF, Lark client, or graph details.
        """
        kind, expected_status, send_operation = self._request_details(request)
        request, uploaded_attachment_tokens = await self._upload_business_attachments(
            request,
            effect_boundary,
        )
        config = {"configurable": {"thread_id": request.email_id}}
        state = await self._graph.aget_state(config)
        if isinstance(request, ApprovalRequest) and effect_boundary is not None:
            request = await self._freeze_approval_payload(
                request,
                state,
                config,
                effect_boundary,
            )

        await self._authorize(
            effect_boundary,
            32,
            {"operation": "generate_notification_pdf", "email_id": request.email_id},
        )
        try:
            pdf_result = await self._generate_pdf(
                request.email_id,
                state,
                dependencies=self._graph_dependencies,
                upload_fn=self._upload_file,
                delete_fn=self._delete_file,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("Review PDF generation failed: error_type=%s", type(exc).__name__)
            await self._record_known_failure(
                request.email_id,
                kind,
                pdf_token=None,
                attachment_tokens=uploaded_attachment_tokens,
                effect_boundary=effect_boundary,
                error_code="notification_pdf_generation_failed",
            )
            return EmailDeliveryOutcome(kind, EmailDeliveryDisposition.KNOWN_FAILURE)

        pdf_stage = await self._stage_review_pdf(
            request.email_id,
            pdf_result,
            effect_boundary,
        )
        if not pdf_stage.ready or pdf_stage.url is None or pdf_stage.new_token is None:
            await self._record_known_failure(
                request.email_id,
                kind,
                pdf_token=pdf_stage.new_token,
                attachment_tokens=uploaded_attachment_tokens,
                effect_boundary=effect_boundary,
                error_code=pdf_stage.error_code or "notification_pdf_stage_failed",
            )
            return EmailDeliveryOutcome(kind, EmailDeliveryDisposition.KNOWN_FAILURE)

        await self._authorize(
            effect_boundary,
            33,
            {"operation": send_operation, "email_id": request.email_id},
        )
        try:
            raw_card_delivery = await asyncio.to_thread(
                self._send_card,
                request,
                pdf_stage.url,
            )
            card_delivery = self._normalize_card_delivery(raw_card_delivery)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error(
                "Lark card transport outcome unknown: kind=%s error_type=%s",
                kind,
                type(exc).__name__,
            )
            card_delivery = LarkCardDelivery(accepted=False, outcome_known=False)

        if not card_delivery.outcome_known:
            try:
                # Quarantine closes the delivery attempt rather than permitting
                # a worker retry to create a second user-visible card.
                await self._advance_effect_committed(effect_boundary)
                await self._move_to_manual_review(
                    request.email_id,
                    DELIVERY_OUTCOME_UNKNOWN_CODE,
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                raise EmailDeliverySideEffectCommittedError(
                    kind=kind,
                    cause=exc,
                ) from None
            return EmailDeliveryOutcome(
                kind,
                EmailDeliveryDisposition.UNKNOWN,
                pdf_token=pdf_stage.new_token,
            )

        if not card_delivery.accepted:
            await self._record_known_failure(
                request.email_id,
                kind,
                pdf_token=pdf_stage.new_token,
                attachment_tokens=uploaded_attachment_tokens,
                effect_boundary=effect_boundary,
                error_code=self._known_failure_code(kind),
            )
            self._record_card_dispatch(kind, delivered=False)
            return EmailDeliveryOutcome(
                kind,
                EmailDeliveryDisposition.KNOWN_FAILURE,
                pdf_token=pdf_stage.new_token,
            )

        try:
            # The visible effect has now happened. The Inbox owner completes
            # this handoff only after its own Exchange mark-as-read succeeds.
            await self._advance_effect_committed(effect_boundary)
            if isinstance(request, ManualReviewNotificationRequest):
                await self._move_to_manual_review(
                    request.email_id,
                    normalize_manual_review_code(request.reason),
                )
            else:
                await self._database.update_status(request.email_id, expected_status)
            cleanup_confirmed = await self._delete_replaced_pdf(
                request.email_id,
                pdf_stage.old_token,
                pdf_stage.new_token,
                effect_boundary,
            )
            if effect_boundary is not None and not cleanup_confirmed:
                raise RuntimeError("replaced_pdf_cleanup_unconfirmed")
        except asyncio.CancelledError as exc:
            raise EmailDeliverySideEffectCommittedError(kind=kind, cause=exc) from None
        except Exception as exc:
            raise EmailDeliverySideEffectCommittedError(kind=kind, cause=exc) from None

        self._record_card_dispatch(kind, delivered=True)
        return EmailDeliveryOutcome(
            kind,
            EmailDeliveryDisposition.CONFIRMED,
            pdf_token=pdf_stage.new_token,
            message_id=card_delivery.message_id,
        )

    @staticmethod
    def _record_card_dispatch(kind: EmailDeliveryKind, *, delivered: bool) -> None:
        """Retain the existing card metric without making it a delivery dependency."""
        try:
            from src.observability.metrics import record_card_dispatch

            record_card_dispatch(kind.value, delivered)
        except Exception:
            pass

    async def _freeze_approval_payload(
        self,
        request: ApprovalRequest,
        state: object,
        config: Mapping[str, object],
        effect_boundary: ExternalEffectBoundary,
    ) -> ApprovalRequest:
        """Append and bind the exact payload shown by the approval card."""
        values = getattr(state, "values", None)
        if not isinstance(values, Mapping):
            raise RuntimeError("approval_checkpoint_unavailable")
        inbox_id = request.inbox_id
        if inbox_id != effect_boundary.scope.inbox_id:
            raise RuntimeError("approval_inbox_mismatch")
        if self._resolve_approval_recipients is None:
            raise RuntimeError("approval_recipient_resolver_unavailable")
        decision = RouteDecision.model_validate(values.get("route_decision"))
        if decision.params.get("include_attachments", False):
            raise RuntimeError("unbound_forward_attachments")
        resolved = await self._resolve_approval_recipients(
            request.email_data.get("draft_to") or values.get("draft_to") or [],
            request.email_data.get("draft_cc") or values.get("draft_cc") or [],
        )
        if resolved is None or not await recipients_follow_route(
            decision,
            request.email_data,
            resolved,
        ):
            raise RuntimeError("recipient_policy_mismatch")
        run = await self._database.get_handoff_run(inbox_id)
        if not run or not run.get("evidence_digest"):
            raise RuntimeError("durable_handoff_unavailable")
        revision = await self._database.create_payload_revision(
            inbox_id=inbox_id,
            expected_version=int(run["version"]),
            expected_payload_revision=None,
            expected_payload_digest=None,
            payload={
                "decision_digest": decision.canonical_digest(),
                "plan_digest": values.get("handoff_plan_digest"),
                "evidence_digest": values.get("evidence_pack_digest"),
                "draft_digest": hashlib.sha256(request.draft.encode("utf-8")).hexdigest(),
                "draft_content": request.draft,
                "draft_ref": {"draft_id": values.get("draft_id")},
                "to": list(resolved.to),
                "cc": list(resolved.cc),
                "attachment_refs": [],
                "attachment_digests": [],
                "external_recipient_acknowledged": True,
                "editor": "system",
                "edited_at": datetime.now(UTC),
            },
        )
        binding = await self._database.get_payload_revision_binding(
            inbox_id=inbox_id,
            revision=revision,
        )
        if binding is None:
            raise RuntimeError("payload_binding_unavailable")
        await self._graph.aupdate_state(
            config,
            {
                "payload_revision": revision,
                "payload_digest": binding["payload_digest"],
            },
        )
        return replace(
            request,
            payload_revision=revision,
            payload_digest=str(binding["payload_digest"]),
        )

    @staticmethod
    async def _authorize(
        boundary: ExternalEffectBoundary | None,
        ordinal: int,
        target: object,
    ) -> None:
        if boundary is not None:
            await boundary.before(ExternalEffectKind.FEISHU, ordinal, target)

    @staticmethod
    def _request_details(
        request: EmailDeliveryRequest,
    ) -> tuple[EmailDeliveryKind, str, str]:
        if isinstance(request, ReadNotificationRequest):
            return (
                EmailDeliveryKind.READ_NOTIFICATION,
                "notified_readonly",
                "send_read_only_card",
            )
        if isinstance(request, ApprovalRequest):
            return (
                EmailDeliveryKind.APPROVAL,
                "waiting_approval",
                "send_approval_card",
            )
        if isinstance(request, ManualReviewNotificationRequest):
            return (
                EmailDeliveryKind.MANUAL_REVIEW,
                "manual_review",
                "send_manual_review_card",
            )
        raise TypeError("unsupported_email_delivery_request")

    @staticmethod
    def _normalize_card_delivery(value: object) -> LarkCardDelivery:
        if isinstance(value, LarkCardDelivery):
            return value
        if isinstance(value, bool):
            return LarkCardDelivery(accepted=value, outcome_known=True)
        # A malformed adapter result is not a safe reason to retry a card.
        return LarkCardDelivery(accepted=False, outcome_known=False)

    @staticmethod
    def _known_failure_code(kind: EmailDeliveryKind) -> str:
        if kind is EmailDeliveryKind.READ_NOTIFICATION:
            return "read_only_card_rejected"
        if kind is EmailDeliveryKind.APPROVAL:
            return "approval_card_rejected"
        return "manual_review_card_rejected"

    async def _record_known_failure(
        self,
        email_id: str,
        kind: EmailDeliveryKind,
        *,
        pdf_token: str | None,
        attachment_tokens: tuple[str, ...],
        effect_boundary: ExternalEffectBoundary | None,
        error_code: str,
    ) -> None:
        await self._database.update_status(
            email_id,
            "delivery_failed",
            error_message=error_code,
        )
        if pdf_token is not None:
            retained = await self._retire_unpublished_pdf(
                email_id,
                pdf_token,
                effect_boundary,
            )
            if effect_boundary is not None and not retained:
                raise RuntimeError("unpublished_pdf_cleanup_unconfirmed")
        for ordinal, token in enumerate(attachment_tokens):
            retained = await self._delete_or_retain_token(
                email_id,
                token,
                effect_boundary,
                ordinal=64 + ordinal,
                operation="cleanup_attachment",
            )
            if effect_boundary is not None and not retained:
                raise RuntimeError("attachment_cleanup_unconfirmed")
        logger.info("Email Feishu Delivery failed with known outcome: kind=%s", kind)

    async def _move_to_manual_review(self, email_id: str, code: str) -> None:
        moved = await self._database.compare_and_set_manual_review(
            email_id,
            expected=MANUAL_REVIEW_SOURCE_STATUSES,
            error_code=code,
        )
        if moved is True:
            return
        status_reader = getattr(self._database, "get_email_status", None)
        if callable(status_reader) and await status_reader(email_id) == "manual_review":
            return
        raise RuntimeError("manual_review_transition_unconfirmed")

    async def _advance_effect_committed(
        self,
        effect_boundary: ExternalEffectBoundary | None,
    ) -> None:
        if effect_boundary is None:
            return
        advance = getattr(self._database, "advance_handoff_execution", None)
        if not callable(advance):
            raise RuntimeError("canonical_handoff_transition_unavailable")
        await advance(
            inbox_id=effect_boundary.scope.inbox_id,
            expected_state="planned",
            next_state="effect_committed",
        )

    async def _stage_review_pdf(
        self,
        email_id: str,
        pdf_result: object,
        effect_boundary: ExternalEffectBoundary | None,
    ) -> _PdfStage:
        """Atomically stage a new active PDF and protect its predecessor."""
        config = {"configurable": {"thread_id": email_id}}
        async with get_graph_resource_lock(email_id):
            if isinstance(pdf_result, PdfFlowOutcome):
                tracked = True
                for token in pdf_result.cleanup_tokens:
                    tracked = (
                        await self._retain_cleanup_token(
                            email_id,
                            token,
                            state_lock_held=True,
                        )
                        and tracked
                    )
                if pdf_result.protected_tokens:
                    try:
                        state = await self._graph.aget_state(config)
                        values = getattr(state, "values", {})
                        known = set(values.get("attachment_tokens") or [])
                        active = values.get("pdf_token")
                        if isinstance(active, str):
                            known.add(active)
                        tracked = tracked and all(
                            token in known for token in pdf_result.protected_tokens
                        )
                    except Exception:
                        tracked = False
                return _PdfStage(
                    ready=False,
                    error_code=(
                        "notification_pdf_reconciliation_required"
                        if tracked
                        else "notification_pdf_cleanup_untracked"
                    ),
                )

            if not isinstance(pdf_result, Mapping):
                return _PdfStage(
                    ready=False,
                    error_code="notification_pdf_generation_failed",
                )
            token = pdf_result.get("file_token")
            url = pdf_result.get("url")
            valid_token = (
                isinstance(token, str) and bool(token) and len(token.encode("utf-8")) <= 512
            )
            valid_url = (
                isinstance(url, str) and bool(url) and len(url.encode("utf-8")) <= 2_048
            )
            if not valid_token or not valid_url:
                if isinstance(token, str) and token:
                    await self._delete_or_retain_token(
                        email_id,
                        token,
                        effect_boundary,
                        ordinal=40,
                        operation="cleanup_invalid_pdf",
                        state_lock_held=True,
                    )
                return _PdfStage(
                    ready=False,
                    error_code="notification_pdf_generation_failed",
                )

            old_token: str | None = None
            try:
                state = await self._graph.aget_state(config)
                values = getattr(state, "values", None)
                if not isinstance(values, Mapping):
                    return _PdfStage(
                        ready=False,
                        error_code="notification_pdf_state_unavailable",
                    )
                values = dict(values)
                old_token = values.get("pdf_token")
                if not isinstance(old_token, str) or not old_token:
                    old_token = None
                cleanup_tokens = list(values.get("attachment_tokens") or [])
                cleanup_tokens = [item for item in cleanup_tokens if item != token]
                if old_token is not None and old_token != token and old_token not in cleanup_tokens:
                    if len(cleanup_tokens) >= MAX_TOKENS:
                        retained = await self._delete_or_retain_token(
                            email_id,
                            token,
                            effect_boundary,
                            ordinal=40,
                            operation="cleanup_replacement_pdf",
                            state_lock_held=True,
                        )
                        return _PdfStage(
                            ready=False,
                            error_code=(
                                "notification_pdf_replacement_capacity_exhausted"
                                if retained
                                else "notification_pdf_cleanup_untracked"
                            ),
                        )
                    cleanup_tokens.append(old_token)
                update = sanitize_graph_delta(
                    values,
                    {"pdf_token": token, "attachment_tokens": cleanup_tokens},
                )
                await self._graph.aupdate_state(config, update)
                confirmed = await self._graph.aget_state(config)
                confirmed_values = getattr(confirmed, "values", None)
                confirmed_tokens = (
                    list(confirmed_values.get("attachment_tokens") or [])
                    if isinstance(confirmed_values, Mapping)
                    else []
                )
                stage_confirmed = (
                    isinstance(confirmed_values, Mapping)
                    and confirmed_values.get("pdf_token") == token
                    and token not in confirmed_tokens
                    and (
                        old_token is None
                        or old_token == token
                        or old_token in confirmed_tokens
                    )
                )
                if stage_confirmed:
                    return _PdfStage(
                        ready=True,
                        url=url,
                        old_token=old_token,
                        new_token=token,
                    )
                return _PdfStage(
                    ready=False,
                    new_token=token,
                    error_code="notification_pdf_state_write_unconfirmed",
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("Review PDF staging failed: error_type=%s", type(exc).__name__)
                try:
                    current = await self._graph.aget_state(config)
                    current_values = getattr(current, "values", {})
                    current_token = current_values.get("pdf_token")
                except Exception:
                    current_values = {}
                    current_token = None
                current_tokens = (
                    list(current_values.get("attachment_tokens") or [])
                    if isinstance(current_values, Mapping)
                    else []
                )
                if current_token == token and (
                    old_token is None
                    or old_token == token
                    or old_token in current_tokens
                ):
                    return _PdfStage(
                        ready=True,
                        url=url,
                        old_token=old_token,
                        new_token=token,
                    )
                if current_token == token:
                    return _PdfStage(
                        ready=False,
                        new_token=token,
                        error_code="notification_pdf_state_write_unconfirmed",
                    )
                retained = await self._delete_or_retain_token(
                    email_id,
                    token,
                    effect_boundary,
                    ordinal=40,
                    operation="cleanup_unstaged_pdf",
                    state_lock_held=True,
                )
                return _PdfStage(
                    ready=False,
                    error_code=(
                        "notification_pdf_state_write_failed"
                        if retained
                        else "notification_pdf_cleanup_untracked"
                    ),
                )

    async def _upload_business_attachments(
        self,
        request: EmailDeliveryRequest,
        effect_boundary: ExternalEffectBoundary | None,
    ) -> tuple[EmailDeliveryRequest, tuple[str, ...]]:
        """Upload only admitted business attachments for approval/read cards."""
        if isinstance(request, ManualReviewNotificationRequest):
            return request, ()

        email_data = deepcopy(dict(request.email_data))
        attachments = email_data.get("attachments")
        if not isinstance(attachments, list):
            return replace(request, email_data=email_data), ()

        attachment_policy = AttachmentPolicy(
            max_bytes=input_limits_from_settings(get_settings()).attachment_single_bytes
        )
        links: list[dict[str, str]] = []
        uploaded_tokens: list[str] = []
        for ordinal, attachment in enumerate(select_business_attachments(email_data)):
            decision = attachment_policy.assess(attachment)
            if not decision.allowed or decision.content is None:
                continue
            content = decision.content
            await self._authorize(
                effect_boundary,
                ordinal,
                {
                    "operation": "upload_attachment",
                    "content_sha256": hashlib.sha256(content).hexdigest(),
                    "size": len(content),
                },
            )
            try:
                uploaded = await asyncio.to_thread(
                    self._upload_file,
                    decision.name,
                    content,
                    len(content),
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "Business attachment upload failed: error_type=%s",
                    type(exc).__name__,
                )
                continue
            token = uploaded.get("file_token") if isinstance(uploaded, Mapping) else None
            if not isinstance(token, str) or not token:
                continue
            retained = await self._retain_cleanup_token(request.email_id, token)
            if not retained:
                deleted = await self._delete_or_retain_token(
                    request.email_id,
                    token,
                    effect_boundary,
                    ordinal=40,
                    operation="cleanup_untracked_attachment",
                )
                if not deleted:
                    raise RuntimeError("attachment_cleanup_handle_untracked")
                continue
            if token not in uploaded_tokens:
                uploaded_tokens.append(token)
            url = uploaded.get("url") if isinstance(uploaded, Mapping) else None
            if isinstance(url, str) and url:
                links.append({"name": decision.name, "lark_file_url": url})

        for attachment in attachments:
            if not isinstance(attachment, dict):
                continue
            for index, link in enumerate(links):
                if link["name"] == str(attachment.get("name", "")):
                    attachment["lark_file_url"] = link["lark_file_url"]
                    links.pop(index)
                    break
        return replace(request, email_data=email_data), tuple(uploaded_tokens)

    async def _retain_cleanup_token(
        self,
        email_id: str,
        token: str,
        *,
        state_lock_held: bool = False,
    ) -> bool:
        """Persist one remote handle before another external effect may follow."""
        if not state_lock_held:
            async with get_graph_resource_lock(email_id):
                return await self._retain_cleanup_token(
                    email_id,
                    token,
                    state_lock_held=True,
                )
        config = {"configurable": {"thread_id": email_id}}
        try:
            state = await self._graph.aget_state(config)
            values = getattr(state, "values", None)
            if not isinstance(values, Mapping):
                return False
            tokens = list(values.get("attachment_tokens") or [])
            if token in tokens:
                return True
            if len(tokens) >= MAX_TOKENS:
                return False
            await self._graph.aupdate_state(
                config,
                sanitize_graph_delta(values, {"attachment_tokens": [*tokens, token]}),
            )
            confirmed = await self._graph.aget_state(config)
            confirmed_values = getattr(confirmed, "values", {})
            return token in (confirmed_values.get("attachment_tokens") or [])
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("Delivery cleanup handle persistence failed: error_type=%s", type(exc).__name__)
            return False

    async def _remove_cleanup_token(
        self,
        email_id: str,
        token: str,
        *,
        state_lock_held: bool = False,
    ) -> None:
        if not state_lock_held:
            async with get_graph_resource_lock(email_id):
                await self._remove_cleanup_token(
                    email_id,
                    token,
                    state_lock_held=True,
                )
            return
        config = {"configurable": {"thread_id": email_id}}
        state = await self._graph.aget_state(config)
        values = getattr(state, "values", None)
        if not isinstance(values, Mapping):
            return
        tokens = [item for item in (values.get("attachment_tokens") or []) if item != token]
        if tokens == list(values.get("attachment_tokens") or []):
            return
        await self._graph.aupdate_state(
            config,
            sanitize_graph_delta(values, {"attachment_tokens": tokens}),
        )

    async def _delete_or_retain_token(
        self,
        email_id: str,
        token: str,
        effect_boundary: ExternalEffectBoundary | None,
        *,
        ordinal: int,
        operation: str,
        state_lock_held: bool = False,
    ) -> bool:
        """Return true only if deletion succeeded or a retry-safe handle exists."""
        retained = await self._retain_cleanup_token(
            email_id,
            token,
            state_lock_held=state_lock_held,
        )
        if not retained:
            return False
        await self._authorize(
            effect_boundary,
            ordinal,
            {
                "operation": operation,
                "token_sha256": hashlib.sha256(token.encode()).hexdigest(),
            },
        )
        try:
            deleted = await asyncio.to_thread(self._delete_file, token)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("Drive cleanup failed: error_type=%s", type(exc).__name__)
            deleted = False
        if deleted:
            await self._remove_cleanup_token(
                email_id,
                token,
                state_lock_held=state_lock_held,
            )
        return True

    async def _retire_unpublished_pdf(
        self,
        email_id: str,
        token: str,
        effect_boundary: ExternalEffectBoundary | None,
    ) -> bool:
        """Make an unsent PDF eligible for cleanup before deleting it."""
        config = {"configurable": {"thread_id": email_id}}
        async with get_graph_resource_lock(email_id):
            state = await self._graph.aget_state(config)
            values = getattr(state, "values", None)
            if not isinstance(values, Mapping):
                return False
            tokens = list(values.get("attachment_tokens") or [])
            if token not in tokens:
                if len(tokens) >= MAX_TOKENS:
                    return False
                tokens.append(token)
            delta: dict[str, object] = {"attachment_tokens": tokens}
            if values.get("pdf_token") == token:
                delta["pdf_token"] = None
            await self._graph.aupdate_state(
                config,
                sanitize_graph_delta(values, delta),
            )
        return await self._delete_or_retain_token(
            email_id,
            token,
            effect_boundary,
            ordinal=40,
            operation="cleanup_unpublished_pdf",
        )

    async def _delete_replaced_pdf(
        self,
        email_id: str,
        old_token: str | None,
        new_token: str,
        effect_boundary: ExternalEffectBoundary | None,
    ) -> bool:
        """Retire only the PDF that the newly confirmed card has replaced."""
        if old_token is None or old_token == new_token:
            return True
        return await self._delete_or_retain_token(
            email_id,
            old_token,
            effect_boundary,
            ordinal=34,
            operation="delete_replaced_pdf",
        )


def build_email_feishu_delivery(
    *,
    database: Any,
    graph: Any,
    graph_dependencies: GraphDependencies,
    lark_api_client: object | None,
    card_builder: object | None,
) -> EmailFeishuDelivery:
    """Compose Email Feishu Delivery from explicit stable runtime dependencies."""
    from src.utils.lark_file_ops import delete_file_from_drive, upload_file_to_drive
    from src.utils.lark_messaging import (
        deliver_approval_card,
        deliver_manual_review_card,
        deliver_read_only_card,
    )
    from src.utils.lark_pdf_flow import generate_and_upload_pdf
    from src.safety.recipients import resolve_recipients

    def upload_file(name: str, content: bytes, size: int) -> object:
        return upload_file_to_drive(
            name,
            content,
            size,
            lark_api_client=lark_api_client,
        )

    def delete_file(token: str) -> bool:
        return delete_file_from_drive(token, lark_api_client=lark_api_client)

    async def generate_pdf(
        email_id: str,
        state: object,
        *,
        dependencies: GraphDependencies,
        upload_fn: DriveUpload,
        delete_fn: DriveDelete,
    ) -> object:
        return await generate_and_upload_pdf(
            email_id,
            state,
            dependencies=dependencies,
            upload_fn=upload_fn,
            delete_fn=delete_fn,
        )

    async def resolve_approval_recipients(
        raw_to: object,
        raw_cc: object,
    ) -> ResolvedRecipients | None:
        return await resolve_recipients(
            raw_to,
            raw_cc,
            lark_client=lark_api_client,
        )

    def send_card(request: EmailDeliveryRequest, pdf_url: str) -> LarkCardDelivery:
        if isinstance(request, ApprovalRequest):
            result = deliver_approval_card(
                request.email_id,
                request.draft,
                list(request.context),
                dict(request.email_data),
                dict(request.classification),
                pdf_url=pdf_url,
                routing_log=list(request.routing_log),
                inbox_id=request.inbox_id,
                payload_revision=request.payload_revision,
                payload_digest=request.payload_digest,
                lark_api_client=lark_api_client,
                card_builder=card_builder,
            )
        elif isinstance(request, ReadNotificationRequest):
            result = deliver_read_only_card(
                request.email_id,
                list(request.context),
                dict(request.email_data),
                dict(request.classification),
                pdf_url=pdf_url,
                routing_log=list(request.routing_log),
                lark_api_client=lark_api_client,
                card_builder=card_builder,
            )
        elif isinstance(request, ManualReviewNotificationRequest):
            result = deliver_manual_review_card(
                request.email_id,
                dict(request.email_data),
                request.reason,
                classification=dict(request.classification),
                pdf_url=pdf_url,
                routing_log=list(request.routing_log),
                lark_api_client=lark_api_client,
                card_builder=card_builder,
            )
        else:
            raise TypeError("unsupported_email_delivery_request")
        return LarkCardDelivery(
            accepted=bool(result.accepted),
            outcome_known=bool(result.outcome_known),
            message_id=result.message_id,
        )

    return EmailFeishuDelivery(
        database=database,
        graph=graph,
        graph_dependencies=graph_dependencies,
        generate_pdf=generate_pdf,
        send_card=send_card,
        upload_file=upload_file,
        delete_file=delete_file,
        resolve_approval_recipients=resolve_approval_recipients,
    )
