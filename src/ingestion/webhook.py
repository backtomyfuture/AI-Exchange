"""Trusted durable Webhook intake with no business-effect dependencies."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal, Protocol

from src.domain.email_state import PipelineGenerationState
from src.ingestion.models import IngressReceipt, IngressSource, PipelineGeneration
from src.ingestion.normalization import (
    VerifiedMailWebhookEnvelope,
    VerifiedTestWebhookEnvelope,
    VerifiedWebhookEnvelope,
    normalize_verified_webhook_request,
    verify_webhook_request,
)
from src.ingestion.policy import ProcessingPolicyResolver


_GREENFIELD_PIPELINE = "durable_v1"


class _ReadySnapshotPort(Protocol):
    async def get_ready_snapshot(self, account_id: int) -> object: ...


class _OwnershipPort(Protocol):
    async def current_ingress(self, account_id: int) -> object: ...


class _InboxPort(Protocol):
    async def insert(
        self,
        event: object,
        generation: int,
        fencing_token: int,
    ) -> object: ...


class WebhookIngressUnavailable(RuntimeError):
    """Fixed safe failure for missing or stale durable intake authority."""

    safe_code = "ingress.webhook_unavailable"
    safe_summary = "Webhook ingress is unavailable"

    def __init__(self) -> None:
        super().__init__(self.safe_summary)

    def __repr__(self) -> str:
        return f"WebhookIngressUnavailable(safe_code={self.safe_code!r})"


@dataclass(frozen=True, slots=True)
class TestWebhookReceipt:
    accepted: Literal[True] = True

    def __post_init__(self) -> None:
        if self.accepted is not True:
            raise ValueError("accepted must be exactly true")


type WebhookAcceptance = IngressReceipt | TestWebhookReceipt


class WebhookIngressService:
    """Accept verified Exchange events into exactly one durable Inbox sink."""

    def __init__(
        self,
        *,
        expected_account_id: int,
        snapshot_provider: _ReadySnapshotPort,
        policy_resolver: ProcessingPolicyResolver,
        ownership_repository: _OwnershipPort,
        inbox_repository: _InboxPort,
    ) -> None:
        if (
            type(expected_account_id) is not int
            or expected_account_id <= 0
            or expected_account_id > 2**63 - 1
        ):
            raise ValueError("expected_account_id must be a positive BIGINT")
        for dependency, method_name in (
            (snapshot_provider, "get_ready_snapshot"),
            (policy_resolver, "resolve"),
            (ownership_repository, "current_ingress"),
            (inbox_repository, "insert"),
        ):
            if not callable(getattr(dependency, method_name, None)):
                raise ValueError("webhook ingress dependency is invalid")
        self._expected_account_id = expected_account_id
        self._snapshot_provider = snapshot_provider
        self._policy_resolver = policy_resolver
        self._ownership_repository = ownership_repository
        self._inbox_repository = inbox_repository

    async def accept(
        self,
        *,
        raw_body: bytes,
        payload: Mapping[str, Any],
        header_event: str | None,
    ) -> WebhookAcceptance:
        verified = verify_webhook_request(
            raw_body=raw_body,
            payload=payload,
            header_event=header_event,
            expected_account_id=self._expected_account_id,
        )
        envelope = verified.envelope
        if type(envelope) is VerifiedTestWebhookEnvelope:
            return TestWebhookReceipt()

        snapshot = await self._snapshot_provider.get_ready_snapshot(envelope.account_id)
        policy = self._policy_resolver.resolve(
            IngressSource.WEBHOOK,
            envelope.raw_event_type,
            envelope.change_kind,
            envelope.exact_folder_identity,
            snapshot,
        )
        event = normalize_verified_webhook_request(
            verified,
            processing_policy=policy,
        )

        generation = await self._ownership_repository.current_ingress(
            envelope.account_id
        )
        if (
            type(generation) is not PipelineGeneration
            or generation.account_id != envelope.account_id
            or generation.pipeline_name != _GREENFIELD_PIPELINE
            or generation.state is not PipelineGenerationState.CURRENT_INGRESS
        ):
            raise WebhookIngressUnavailable()

        receipt = await self._inbox_repository.insert(
            event,
            generation.generation,
            generation.fencing_token,
        )
        if type(receipt) is not IngressReceipt:
            raise WebhookIngressUnavailable()
        return receipt


__all__ = [
    "TestWebhookReceipt",
    "VerifiedMailWebhookEnvelope",
    "VerifiedTestWebhookEnvelope",
    "VerifiedWebhookEnvelope",
    "WebhookAcceptance",
    "WebhookIngressService",
    "WebhookIngressUnavailable",
]
