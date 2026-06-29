"""派发策略：根据邮件分类与收件人，决定飞书通知类型。

只依赖 config，保持轻量，便于单元测试（不引入 lark_app 等重依赖）。
"""
from src.config import get_settings


def is_direct_recipient(email: dict, me: str | None = None) -> bool:
    """配置邮箱是否出现在邮件 To 收件人中（子串、大小写不敏感）。

    `me` 为空时从 EXCHANGE_ACCOUNT_EMAIL 读取；仍为空则兜底返回 True。
    """
    if me is None:
        me = get_settings().EXCHANGE_ACCOUNT_EMAIL or ""
    me = me.lower()
    if not me:
        return True

    to_list = email.get("to") or []
    if isinstance(to_list, str):
        to_list = [to_list]
    return any(me in str(t).lower() for t in to_list)


def is_vip_sender(email: dict) -> bool:
    """发件人是否在领导/VIP 名单（LEADER_SENDERS, CSV）中。"""
    leaders = [
        s.strip().lower()
        for s in (get_settings().LEADER_SENDERS or "").split(",")
        if s.strip()
    ]
    if not leaders:
        return False
    sender = str(email.get("sender") or "").lower()
    return any(leader in sender for leader in leaders)


def decide_notification_kind(classification: dict, email: dict) -> str:
    """返回 'approval' | 'read_only' | 'skipped'。

    需回复 → approval；否则按「值得阅读」规则决定 read_only / skipped。
    """
    if classification.get("need_reply"):
        return "approval"

    if classification.get("intent") == "垃圾邮件":
        return "skipped"
    if is_direct_recipient(email):
        return "read_only"
    if is_vip_sender(email):
        return "read_only"
    if classification.get("priority") in ("P0", "P1"):
        return "read_only"
    return "skipped"
