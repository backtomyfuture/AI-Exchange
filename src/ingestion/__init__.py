"""Durable ingestion domain boundary."""

from src.ingestion.email_events import (
    EMAIL_STATUS_TRANSITIONS,
    EmailEventApplication,
    EmailEventDecision,
    EmailEventDisposition,
    EmailEventReason,
    EmailStatus,
    decide_email_event,
)
from src.ingestion.normalization import (
    normalize_sync_change,
    normalize_webhook_event,
    validate_sync_change_contract,
)
from src.ingestion.ownership import (
    PipelineOwnershipRepository,
    PipelineRetirementBlocked,
    RetirementBlockCode,
    RetirementGuard,
)
from src.ingestion.models import (
    ChangeKind,
    InboxDisposition,
    InboxDispositionStatus,
    InboxLease,
    InboxStats,
    InboxStatus,
    IngressReceipt,
    IngressSource,
    NormalizedIngressEvent,
    PipelineGeneration,
    PipelineGenerationState,
    ProcessingPolicy,
    SyncBatch,
    SyncChange,
    SyncCursorStatus,
)
from src.ingestion.repository import EmailEventTransaction, InboxRepository

__all__ = [
    "ChangeKind",
    "EMAIL_STATUS_TRANSITIONS",
    "EmailEventApplication",
    "EmailEventDecision",
    "EmailEventDisposition",
    "EmailEventReason",
    "EmailEventTransaction",
    "EmailStatus",
    "InboxDisposition",
    "InboxDispositionStatus",
    "InboxLease",
    "InboxRepository",
    "InboxStats",
    "InboxStatus",
    "IngressReceipt",
    "IngressSource",
    "NormalizedIngressEvent",
    "PipelineGeneration",
    "PipelineGenerationState",
    "PipelineOwnershipRepository",
    "PipelineRetirementBlocked",
    "ProcessingPolicy",
    "RetirementBlockCode",
    "RetirementGuard",
    "SyncBatch",
    "SyncChange",
    "SyncCursorStatus",
    "decide_email_event",
    "normalize_sync_change",
    "normalize_webhook_event",
    "validate_sync_change_contract",
]
