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
