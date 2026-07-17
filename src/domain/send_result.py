"""Typed, privacy-safe outcomes for Exchange send operations."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class ExchangeSendOutcome(StrEnum):
    """What the caller may safely conclude after one remote send attempt."""

    SENT = "sent"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class ExchangeSendResult:
    """A bounded result that never carries response or message content."""

    outcome: ExchangeSendOutcome
    status_code: int | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.outcome, ExchangeSendOutcome):
            raise TypeError("outcome must be an ExchangeSendOutcome")
        if self.status_code is not None and (
            type(self.status_code) is not int or not 100 <= self.status_code <= 599
        ):
            raise ValueError("invalid Exchange send status code")
        if self.outcome is ExchangeSendOutcome.SENT and self.status_code != 200:
            raise ValueError("sent outcome requires the confirmed status code")

    @classmethod
    def sent(cls) -> ExchangeSendResult:
        return cls(ExchangeSendOutcome.SENT, status_code=200)

    @classmethod
    def unknown(cls, *, status_code: int | None = None) -> ExchangeSendResult:
        return cls(ExchangeSendOutcome.UNKNOWN, status_code=status_code)

    @property
    def confirmed_sent(self) -> bool:
        return self.outcome is ExchangeSendOutcome.SENT

    @property
    def safe_code(self) -> str:
        if self.confirmed_sent:
            return "exchange.send.confirmed"
        return "exchange.send.outcome_unknown"
