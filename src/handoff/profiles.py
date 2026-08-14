from __future__ import annotations

from types import MappingProxyType

from src.handoff.models import HandoffProfile

_PROFILES = {
    "generic_reply_v1": HandoffProfile(
        profile_id="generic_reply_v1", optional_sources=("mail_thread", "semantic_history")
    ),
    "generic_forward_v1": HandoffProfile(
        profile_id="generic_forward_v1",
        optional_sources=("mail_thread", "semantic_history"),
        writer_mode="fixed",
        fixed_draft="呈阅",
    ),
    # A real business profile: the deterministic rule selects the writing
    # contract, while the read-only mail corpus supplies optional evidence.
    # Lack of history must not make every first VIP conversation fail.
    "vip_direct_reply_v1": HandoffProfile(
        profile_id="vip_direct_reply_v1",
        optional_sources=("exchange_contact", "mail_thread", "semantic_history"),
        writer_mode="llm",
        prompt_modifier=(
            "【VIP 直发写作合同】先明确回应本轮问题或任务，再给出下一步与时间承诺；"
            "信息不足时明确列出待确认项，不得编造日期、状态、金额或责任人。"
        ),
    ),
}
PROFILE_REGISTRY = MappingProxyType(_PROFILES)


def get_handoff_profile(profile_id: str) -> HandoffProfile:
    try:
        return PROFILE_REGISTRY[profile_id]
    except KeyError:
        raise KeyError(f"unknown handoff profile: {profile_id}") from None


__all__ = ["PROFILE_REGISTRY", "get_handoff_profile"]
