from enum import StrEnum


class ErrorKind(StrEnum):
    VALIDATION = "validation_error"
    AUTHENTICATION = "authentication_error"
    RATE_LIMITED = "rate_limited"
    TRANSIENT_DEPENDENCY = "transient_dependency_error"
    PERMANENT_DEPENDENCY = "permanent_dependency_error"
    POLICY_REJECTED = "policy_rejected"
    SEND_UNKNOWN = "send_unknown"
    INTERNAL_INVARIANT = "internal_invariant_error"


class StaleFence(RuntimeError):
    """Fixed, privacy-safe rejection for a non-authoritative pipeline stamp."""

    kind = ErrorKind.INTERNAL_INVARIANT
    safe_code = "pipeline.stale_fence"
    safe_summary = "Pipeline fence is stale"

    def __init__(self) -> None:
        super().__init__(self.safe_summary)

    def __repr__(self) -> str:
        return f"StaleFence(safe_code={self.safe_code!r})"


class IngressValidationCode(StrEnum):
    INVALID_BODY = "ingress.invalid_body"
    BODY_PAYLOAD_MISMATCH = "ingress.body_payload_mismatch"
    EVENT_MISSING = "ingress.event_missing"
    EVENT_CONFLICT = "ingress.event_conflict"
    EVENT_UNSUPPORTED = "ingress.event_unsupported"
    HEADER_EVENT_MISMATCH = "ingress.header_event_mismatch"
    ACCOUNT_INVALID = "ingress.account_invalid"
    EMAIL_ID_INVALID = "ingress.email_id_invalid"
    EMAIL_ID_CONFLICT = "ingress.email_id_conflict"
    FOLDER_INVALID = "ingress.folder_invalid"
    FOLDER_CONFLICT = "ingress.folder_conflict"
    VERSION_INVALID = "ingress.version_invalid"
    VERSION_CONFLICT = "ingress.version_conflict"
    TIMESTAMP_INVALID = "ingress.timestamp_invalid"
    POLICY_INVALID = "ingress.policy_invalid"
    SYNC_CHANGE_INVALID = "ingress.sync_change_invalid"
    SYNC_ITEM_INVALID = "ingress.sync_item_invalid"
    SYNC_ITEM_ID_CONFLICT = "ingress.sync_item_id_conflict"
    CURSOR_INVALID = "ingress.cursor_invalid"
    NORMALIZED_EVENT_INVALID = "ingress.normalized_event_invalid"
    CANONICALIZATION_INVALID = "ingress.canonicalization_invalid"


class IngressValidationError(ValueError):
    """Privacy-safe validation failure at the durable intake boundary."""

    kind = ErrorKind.VALIDATION
    safe_summary = "Invalid ingress event"

    def __init__(self, safe_code: IngressValidationCode) -> None:
        if not isinstance(safe_code, IngressValidationCode):
            raise TypeError("safe_code must be an IngressValidationCode")
        self.safe_code = safe_code
        super().__init__(self.safe_summary)

    def __repr__(self) -> str:
        return f"IngressValidationError(safe_code={self.safe_code.value!r})"


class DatabaseOperationError(RuntimeError):
    def __init__(self, *, operation: str, retryable: bool, message: str):
        super().__init__(message)
        self.operation = operation
        self.retryable = retryable


class ManualReviewRequired(RuntimeError):
    def __init__(self, *, reason: str, safe_summary: str):
        super().__init__(safe_summary)
        self.reason = reason
        self.safe_summary = safe_summary
