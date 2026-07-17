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


class _ExchangeSyncError(RuntimeError):
    """Fixed-shape error for the untrusted Exchange Sync boundary."""

    safe_code = "exchange.sync.failure"
    safe_summary = "Exchange sync failed"
    kind = ErrorKind.PERMANENT_DEPENDENCY
    _retryable = False
    _instance_fields: frozenset[str] = frozenset()

    def __init__(self) -> None:
        super().__init__(self.safe_summary)

    @property
    def retryable(self) -> bool:
        return self._retryable

    def __setattr__(self, name: str, value: object) -> None:
        if name == "__traceback__":
            super().__setattr__(name, value)
            return
        if name not in self._instance_fields or name in vars(self):
            raise AttributeError("Exchange sync errors are immutable")
        super().__setattr__(name, value)

    def __repr__(self) -> str:
        return f"{type(self).__name__}(safe_code={self.safe_code!r})"


class SyncAuthorizationError(_ExchangeSyncError):
    safe_code = "exchange.sync.authorization_failed"
    safe_summary = "Exchange sync authorization failed"
    kind = ErrorKind.AUTHENTICATION


class SyncCursorInvalidError(_ExchangeSyncError):
    safe_code = "exchange.sync.cursor_invalid"
    safe_summary = "Exchange sync cursor is invalid"


class SyncTransientError(_ExchangeSyncError):
    safe_code = "exchange.sync.transient_failure"
    safe_summary = "Exchange sync is temporarily unavailable"
    kind = ErrorKind.TRANSIENT_DEPENDENCY
    _retryable = True
    _instance_fields = frozenset({"retry_after_seconds"})

    def __init__(self, *, retry_after_seconds: int | None = None) -> None:
        if retry_after_seconds is not None and (
            type(retry_after_seconds) is not int
            or not 0 <= retry_after_seconds <= 3600
        ):
            raise TypeError("invalid retry_after_seconds")
        super().__init__()
        self.retry_after_seconds = retry_after_seconds

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}(safe_code={self.safe_code!r}, "
            f"retry_after_seconds={self.retry_after_seconds!r})"
        )


class SyncContractError(_ExchangeSyncError):
    safe_code = "exchange.sync.contract_invalid"
    safe_summary = "Exchange sync contract is invalid"
