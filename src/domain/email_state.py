from enum import StrEnum


class InitialEmailWriteResult(StrEnum):
    CREATED = "created"
    DUPLICATE = "duplicate"


class ProcessingOutcome(StrEnum):
    PROCESSED = "processed"
    FAILED = "failed"
    DUPLICATE = "duplicate"
    ARCHIVED = "archived"
    MANUAL_REVIEW = "manual_review"


class PipelineGenerationState(StrEnum):
    CURRENT_INGRESS = "current_ingress"
    QUIESCING = "quiescing"
    DRAINING = "draining"
    RETIRED = "retired"


SAFE_DUPLICATE_READ_STATUSES = frozenset(
    {"waiting_approval", "notified_readonly", "skipped", "sent"}
)
