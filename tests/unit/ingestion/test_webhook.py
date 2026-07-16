from __future__ import annotations

import inspect
import json
from dataclasses import fields, is_dataclass
from typing import Any, get_args

import pytest

from src.domain.email_state import PipelineGenerationState
from src.domain.errors import IngressValidationCode, IngressValidationError
from src.ingestion.models import (
    ChangeKind,
    IngressReceipt,
    IngressSource,
    PipelineGeneration,
    ProcessingPolicy,
)
from src.ingestion.policy import (
    FolderScope,
    PolicySnapshot,
    PolicySnapshotUnavailableError,
    ProcessingPolicyResolver,
)
from src.ingestion.webhook import (
    TestWebhookReceipt as _TestWebhookReceipt,
    VerifiedMailWebhookEnvelope,
    VerifiedTestWebhookEnvelope,
    VerifiedWebhookEnvelope,
    WebhookIngressService,
    WebhookIngressUnavailable,
)


_INBOX_ID = "00000000-0000-4000-8000-000000000001"
_DEFAULT = object()


def _policy_matrix(
    create_policy: ProcessingPolicy = ProcessingPolicy.FULL,
) -> dict[tuple[IngressSource, str, ChangeKind], ProcessingPolicy]:
    return {
        (
            IngressSource.WEBHOOK,
            "NewMailEvent",
            ChangeKind.CREATE,
        ): create_policy,
        (
            IngressSource.WEBHOOK,
            "CreatedEvent",
            ChangeKind.CREATE,
        ): ProcessingPolicy.IGNORED,
        (
            IngressSource.WEBHOOK,
            "ModifiedEvent",
            ChangeKind.UPDATE,
        ): ProcessingPolicy.METADATA_ONLY,
        (
            IngressSource.WEBHOOK,
            "DeletedEvent",
            ChangeKind.DELETE,
        ): ProcessingPolicy.METADATA_ONLY,
        (IngressSource.SYNC, "create", ChangeKind.CREATE): create_policy,
        (
            IngressSource.SYNC,
            "update",
            ChangeKind.UPDATE,
        ): ProcessingPolicy.METADATA_ONLY,
        (
            IngressSource.SYNC,
            "delete",
            ChangeKind.DELETE,
        ): ProcessingPolicy.METADATA_ONLY,
    }


def _snapshot(
    *,
    webhook_id: str = "INBOX",
    create_policy: ProcessingPolicy = ProcessingPolicy.FULL,
) -> PolicySnapshot:
    return PolicySnapshot(
        scopes=(
            FolderScope.configured(
                canonical_key="INBOX",
                webhook_ids=(webhook_id,),
                sync_folder="Inbox",
                event_policy_matrix=_policy_matrix(create_policy),
            ),
        )
    )


def _mail_payload(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "account_id": 8,
        "event": "NewMailEvent",
        "timestamp": 1_752_384_245,
        "item_id": {"id": "message-1", "changekey": "version-1"},
        "parent_folder_id": {"id": "INBOX"},
        "message": "new mail",
    }
    payload.update(overrides)
    return payload


def _test_payload(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "event": "TestEvent",
        "timestamp": 1_752_384_245,
        "account_id": 8,
        "message": "Webhook test successful",
    }
    payload.update(overrides)
    return payload


def _raw(payload: object) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


class _SnapshotProvider:
    def __init__(self, snapshot: object, events: list[str]) -> None:
        self.snapshot = snapshot
        self.events = events

    async def get_ready_snapshot(self, account_id: int) -> object:
        self.events.append(f"policy:{account_id}")
        if isinstance(self.snapshot, BaseException):
            raise self.snapshot
        return self.snapshot


class _OwnershipRepository:
    def __init__(self, generation: object, events: list[str]) -> None:
        self.generation = generation
        self.events = events

    async def current_ingress(self, account_id: int) -> object:
        self.events.append(f"authority:{account_id}")
        if isinstance(self.generation, BaseException):
            raise self.generation
        return self.generation


class _InboxRepository:
    def __init__(
        self,
        receipts: tuple[IngressReceipt, ...],
        events: list[str],
    ) -> None:
        self.receipts = list(receipts)
        self.events = events
        self.calls: list[tuple[object, int, int]] = []

    async def insert(
        self,
        event: object,
        generation: int,
        fencing_token: int,
    ) -> IngressReceipt:
        self.events.append("insert")
        self.calls.append((event, generation, fencing_token))
        return self.receipts.pop(0)


def _current_generation(
    *,
    account_id: int = 8,
    pipeline_name: str = "durable_v1",
    state: PipelineGenerationState = PipelineGenerationState.CURRENT_INGRESS,
) -> PipelineGeneration:
    return PipelineGeneration(
        account_id=account_id,
        generation=3,
        pipeline_name=pipeline_name,
        state=state,
        fencing_token=7,
    )


def _service(
    *,
    events: list[str],
    snapshot: object = _DEFAULT,
    generation: object = _DEFAULT,
    receipts: tuple[IngressReceipt, ...] | None = None,
) -> tuple[WebhookIngressService, _InboxRepository]:
    repository = _InboxRepository(
        receipts or (IngressReceipt(inbox_id=_INBOX_ID, duplicate=False),),
        events,
    )
    service = WebhookIngressService(
        expected_account_id=8,
        snapshot_provider=_SnapshotProvider(
            _snapshot() if snapshot is _DEFAULT else snapshot,
            events,
        ),
        policy_resolver=ProcessingPolicyResolver(),
        ownership_repository=_OwnershipRepository(
            _current_generation() if generation is _DEFAULT else generation,
            events,
        ),
        inbox_repository=repository,
    )
    return service, repository


def test_webhook_contract_types_are_exact_frozen_slotted_values() -> None:
    expected_mail_fields = (
        "account_id",
        "raw_event_type",
        "change_kind",
        "external_email_id",
        "exact_folder_identity",
        "source_version",
        "source_event_at",
    )

    assert is_dataclass(VerifiedMailWebhookEnvelope)
    assert VerifiedMailWebhookEnvelope.__dataclass_params__.frozen is True
    assert tuple(field.name for field in fields(VerifiedMailWebhookEnvelope)) == (
        expected_mail_fields
    )
    assert frozenset(VerifiedMailWebhookEnvelope.__slots__) == frozenset(
        expected_mail_fields
    )

    assert is_dataclass(VerifiedTestWebhookEnvelope)
    assert VerifiedTestWebhookEnvelope.__dataclass_params__.frozen is True
    assert tuple(field.name for field in fields(VerifiedTestWebhookEnvelope)) == (
        "account_id",
    )
    assert VerifiedTestWebhookEnvelope.__slots__ == ("account_id",)

    assert is_dataclass(_TestWebhookReceipt)
    assert _TestWebhookReceipt.__dataclass_params__.frozen is True
    assert tuple(field.name for field in fields(_TestWebhookReceipt)) == ("accepted",)
    assert _TestWebhookReceipt().accepted is True
    with pytest.raises(ValueError, match="exactly true"):
        _TestWebhookReceipt(accepted=False)  # type: ignore[arg-type]
    assert frozenset(get_args(VerifiedWebhookEnvelope)) == frozenset(
        {VerifiedMailWebhookEnvelope, VerifiedTestWebhookEnvelope}
    )


def test_accept_has_only_the_frozen_keyword_only_transport_interface() -> None:
    constructor = inspect.signature(WebhookIngressService)
    signature = inspect.signature(WebhookIngressService.accept)

    assert tuple(constructor.parameters) == (
        "expected_account_id",
        "snapshot_provider",
        "policy_resolver",
        "ownership_repository",
        "inbox_repository",
    )
    assert tuple(signature.parameters) == (
        "self",
        "raw_body",
        "payload",
        "header_event",
    )
    assert signature.parameters["raw_body"].kind is inspect.Parameter.KEYWORD_ONLY
    assert signature.parameters["payload"].kind is inspect.Parameter.KEYWORD_ONLY
    assert signature.parameters["header_event"].kind is inspect.Parameter.KEYWORD_ONLY
    assert all(
        signature.parameters[name].default is inspect.Parameter.empty
        for name in ("raw_body", "payload", "header_event")
    )


@pytest.mark.parametrize("expected_account_id", [True, 0, -1, 2**63, "8"])
def test_service_rejects_invalid_configured_account(
    expected_account_id: object,
) -> None:
    events: list[str] = []

    with pytest.raises(ValueError, match="positive BIGINT"):
        WebhookIngressService(
            expected_account_id=expected_account_id,  # type: ignore[arg-type]
            snapshot_provider=_SnapshotProvider(_snapshot(), events),
            policy_resolver=ProcessingPolicyResolver(),
            ownership_repository=_OwnershipRepository(
                _current_generation(),
                events,
            ),
            inbox_repository=_InboxRepository(
                (IngressReceipt(inbox_id=_INBOX_ID, duplicate=False),),
                events,
            ),
        )


@pytest.mark.parametrize(
    "dependency_name",
    [
        "snapshot_provider",
        "policy_resolver",
        "ownership_repository",
        "inbox_repository",
    ],
)
def test_service_rejects_missing_structural_port(dependency_name: str) -> None:
    events: list[str] = []
    dependencies: dict[str, object] = {
        "snapshot_provider": _SnapshotProvider(_snapshot(), events),
        "policy_resolver": ProcessingPolicyResolver(),
        "ownership_repository": _OwnershipRepository(
            _current_generation(),
            events,
        ),
        "inbox_repository": _InboxRepository(
            (IngressReceipt(inbox_id=_INBOX_ID, duplicate=False),),
            events,
        ),
    }
    dependencies[dependency_name] = object()

    with pytest.raises(ValueError, match="dependency is invalid"):
        WebhookIngressService(
            expected_account_id=8,
            **dependencies,  # type: ignore[arg-type]
        )


def test_unavailable_error_has_fixed_safe_representation() -> None:
    error = WebhookIngressUnavailable()

    assert str(error) == "Webhook ingress is unavailable"
    assert repr(error) == (
        "WebhookIngressUnavailable(safe_code='ingress.webhook_unavailable')"
    )


@pytest.mark.asyncio
async def test_exact_test_event_returns_typed_receipt_without_any_port_call() -> None:
    events: list[str] = []
    service, repository = _service(events=events)
    payload = _test_payload()

    receipt = await service.accept(
        raw_body=_raw(payload),
        payload=payload,
        header_event="TestEvent",
    )

    assert type(receipt) is _TestWebhookReceipt
    assert receipt.accepted is True
    assert events == []
    assert repository.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        _test_payload(account_id=9),
        _test_payload(extra="unsigned compatibility field"),
        _test_payload(message=""),
    ],
)
async def test_test_event_validates_exact_account_and_limited_shape_before_ports(
    payload: dict[str, Any],
) -> None:
    events: list[str] = []
    service, repository = _service(events=events)

    with pytest.raises(IngressValidationError):
        await service.accept(
            raw_body=_raw(payload),
            payload=payload,
            header_event="TestEvent",
        )

    assert events == []
    assert repository.calls == []


@pytest.mark.asyncio
async def test_mail_intake_orders_account_policy_authority_then_single_insert() -> None:
    events: list[str] = []
    service, repository = _service(events=events)
    payload = _mail_payload()

    receipt = await service.accept(
        raw_body=_raw(payload),
        payload=payload,
        header_event="NewMailEvent",
    )

    assert receipt == IngressReceipt(inbox_id=_INBOX_ID, duplicate=False)
    assert events == ["policy:8", "authority:8", "insert"]
    assert len(repository.calls) == 1
    event, generation, fencing_token = repository.calls[0]
    assert event.account_id == 8
    assert event.raw_event_type == "NewMailEvent"
    assert event.kind is ChangeKind.CREATE
    assert event.external_email_id == "message-1"
    assert event.folder == "INBOX"
    assert event.processing_policy is ProcessingPolicy.FULL
    assert generation == 3
    assert fencing_token == 7


@pytest.mark.asyncio
async def test_duplicate_receipt_is_returned_without_changing_normalized_identity() -> (
    None
):
    events: list[str] = []
    first = IngressReceipt(inbox_id=_INBOX_ID, duplicate=False)
    duplicate = IngressReceipt(inbox_id=_INBOX_ID, duplicate=True)
    service, repository = _service(
        events=events,
        receipts=(first, duplicate),
    )
    original = _mail_payload(message="first delivery metadata")
    redelivery = _mail_payload(message="changed delivery metadata")

    first_result = await service.accept(
        raw_body=_raw(original),
        payload=original,
        header_event="NewMailEvent",
    )
    duplicate_result = await service.accept(
        raw_body=_raw(redelivery),
        payload=redelivery,
        header_event="NewMailEvent",
    )

    assert first_result is first
    assert duplicate_result is duplicate
    assert events == [
        "policy:8",
        "authority:8",
        "insert",
        "policy:8",
        "authority:8",
        "insert",
    ]
    first_event, first_generation, first_fence = repository.calls[0]
    second_event, second_generation, second_fence = repository.calls[1]
    assert second_event.dedupe_key == first_event.dedupe_key
    assert (
        second_event.account_id,
        second_event.raw_event_type,
        second_event.kind,
        second_event.external_email_id,
        second_event.folder,
        second_event.source_version,
        second_generation,
        second_fence,
    ) == (
        first_event.account_id,
        first_event.raw_event_type,
        first_event.kind,
        first_event.external_email_id,
        first_event.folder,
        first_event.source_version,
        first_generation,
        first_fence,
    )


@pytest.mark.asyncio
async def test_unknown_ready_folder_is_durably_inserted_as_ignored() -> None:
    events: list[str] = []
    service, repository = _service(events=events)
    payload = _mail_payload(parent_folder_id={"id": "UNKNOWN-FOLDER"})

    receipt = await service.accept(
        raw_body=_raw(payload),
        payload=payload,
        header_event="NewMailEvent",
    )

    assert receipt.duplicate is False
    assert events == ["policy:8", "authority:8", "insert"]
    event, _, _ = repository.calls[0]
    assert event.folder == "UNKNOWN-FOLDER"
    assert event.processing_policy is ProcessingPolicy.IGNORED


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("payload", "raw_body", "header_event", "safe_code"),
    [
        (
            _mail_payload(account_id=9),
            None,
            "NewMailEvent",
            IngressValidationCode.ACCOUNT_INVALID,
        ),
        (
            _mail_payload(),
            None,
            "ModifiedEvent",
            IngressValidationCode.HEADER_EVENT_MISMATCH,
        ),
        (
            _mail_payload(),
            _raw(_mail_payload(account_id=9)),
            "NewMailEvent",
            IngressValidationCode.BODY_PAYLOAD_MISMATCH,
        ),
        (
            _mail_payload(parent_folder_id={"id": ""}),
            None,
            "NewMailEvent",
            IngressValidationCode.FOLDER_INVALID,
        ),
        (
            _mail_payload(event="FutureMailEvent"),
            None,
            "FutureMailEvent",
            IngressValidationCode.EVENT_UNSUPPORTED,
        ),
        (
            _mail_payload(),
            b'{"event":"NewMailEvent","account_id":8,"account_id":9}',
            "NewMailEvent",
            IngressValidationCode.INVALID_BODY,
        ),
        (
            _mail_payload(),
            b'{"event":"NewMailEvent","account_id":NaN}',
            "NewMailEvent",
            IngressValidationCode.INVALID_BODY,
        ),
        (
            _mail_payload(),
            b"not-json",
            "NewMailEvent",
            IngressValidationCode.INVALID_BODY,
        ),
    ],
)
async def test_untrusted_envelope_failures_short_circuit_every_port(
    payload: dict[str, Any],
    raw_body: bytes | None,
    header_event: str,
    safe_code: IngressValidationCode,
) -> None:
    events: list[str] = []
    service, repository = _service(events=events)

    with pytest.raises(IngressValidationError) as caught:
        await service.accept(
            raw_body=_raw(payload) if raw_body is None else raw_body,
            payload=payload,
            header_event=header_event,
        )

    assert caught.value.safe_code is safe_code
    assert events == []
    assert repository.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "snapshot",
    [
        PolicySnapshot.failed(),
        PolicySnapshot(scopes=(), refreshed=False),
    ],
)
async def test_unready_policy_fails_before_authority_and_insert(
    snapshot: PolicySnapshot,
) -> None:
    events: list[str] = []
    service, repository = _service(events=events, snapshot=snapshot)
    payload = _mail_payload()

    with pytest.raises(PolicySnapshotUnavailableError):
        await service.accept(
            raw_body=_raw(payload),
            payload=payload,
            header_event="NewMailEvent",
        )

    assert events == ["policy:8"]
    assert repository.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "generation",
    [
        None,
        _current_generation(account_id=9),
        _current_generation(pipeline_name="legacy_compat"),
        _current_generation(state=PipelineGenerationState.QUIESCING),
    ],
)
async def test_invalid_current_authority_fails_before_insert(
    generation: object,
) -> None:
    events: list[str] = []
    service, repository = _service(events=events, generation=generation)
    payload = _mail_payload()

    with pytest.raises((RuntimeError, ValueError)):
        await service.accept(
            raw_body=_raw(payload),
            payload=payload,
            header_event="NewMailEvent",
        )

    assert events == ["policy:8", "authority:8"]
    assert repository.calls == []


@pytest.mark.asyncio
async def test_invalid_repository_receipt_fails_closed() -> None:
    events: list[str] = []
    service, repository = _service(events=events)
    repository.receipts = [object()]  # type: ignore[list-item]
    payload = _mail_payload()

    with pytest.raises(WebhookIngressUnavailable):
        await service.accept(
            raw_body=_raw(payload),
            payload=payload,
            header_event="NewMailEvent",
        )

    assert events == ["policy:8", "authority:8", "insert"]
    assert len(repository.calls) == 1


@pytest.mark.asyncio
async def test_success_duplicate_test_and_validation_paths_expose_only_structural_ports() -> (
    None
):
    events: list[str] = []
    service, repository = _service(
        events=events,
        receipts=(
            IngressReceipt(inbox_id=_INBOX_ID, duplicate=False),
            IngressReceipt(inbox_id=_INBOX_ID, duplicate=True),
        ),
    )
    mail = _mail_payload()
    test = _test_payload()

    await service.accept(
        raw_body=_raw(test),
        payload=test,
        header_event="TestEvent",
    )
    await service.accept(
        raw_body=_raw(mail),
        payload=mail,
        header_event="NewMailEvent",
    )
    await service.accept(
        raw_body=_raw(mail),
        payload=mail,
        header_event="NewMailEvent",
    )
    with pytest.raises(IngressValidationError):
        await service.accept(
            raw_body=_raw(mail),
            payload=mail,
            header_event="DeletedEvent",
        )

    assert events == [
        "policy:8",
        "authority:8",
        "insert",
        "policy:8",
        "authority:8",
        "insert",
    ]
    assert len(repository.calls) == 2
