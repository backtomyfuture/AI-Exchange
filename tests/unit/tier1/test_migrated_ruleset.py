"""Ruleset-level regression coverage for the migrated ``tier1_rules/`` registry
(docs/tier1-routing-design.md §8: "Ruleset 级回归语料"). Complements the
per-file fixture replay the compiler already runs by asserting the full
compiled artifact's outcome/route for representative emails, including cases
that only a whole-registry evaluation (not a single rule in isolation) can
catch: cross-rule conflicts, the $ME split's mutual exclusivity, the
non-VIP direct-recipient fallback, and group-mailbox address-list membership.

Any change to ``match``/``decision`` in ``tier1_rules/`` must keep this test
green; a change that requires updating an assertion here is exactly the kind
of ruleset-wide impact §8 calls out.
"""
from pathlib import Path

from src.router.tier1.compiler import CompiledArtifact, compile_registry
from src.router.tier1.decision import DecisionOrigin, EvaluationOutcome, build_tier1_decision
from src.router.tier1.dsl import EmailView
from src.router.tier1.schema import CanonicalRoute

RULE_DIR = Path(__file__).resolve().parents[3] / "tier1_rules"
ME_EMAIL = "q-fu@tianjin-air.com"
INTERNAL_DOMAINS = ["tianjin-air.com", "hnair.com", "hnaaviation.com"]


def _compile():
    result = compile_registry(RULE_DIR, internal_email_domains=INTERNAL_DOMAINS, me_email=ME_EMAIL)
    assert isinstance(result, CompiledArtifact), getattr(result, "errors", result)
    return result


def _rules(artifact):
    return [c.manifest for c in artifact.rules]


def test_migrated_registry_compiles():
    artifact = _compile()
    assert {r.rule_id for r in _rules(artifact)} == {
        "T1-INTERNAL-DISTLIST-SILENCE-001",
        "T1-INTERNAL-DISTLIST-READONLY-001",
        "T1-SAFETY-PLATFORM-ARCHIVE-001",
        "T1-HNAIR-MARKETING-ARCHIVE-001",
        "T1-ZHANGXIA-FORWARD-READONLY-001",
        "T1-VIP-DIRECT-REPLY-001",
        "T1-VIP-CC-ONLY-READONLY-001",
        "T1-NON-VIP-DIRECT-READONLY-001",
    }


def test_group_mailbox_email_is_no_action():
    artifact = _compile()
    view = EmailView(sender_address="ops@hnair.com", to_addresses=["tjhkgh@tianjin-air.com"])
    decision = build_tier1_decision(_rules(artifact), view, me_email=ME_EMAIL)
    assert decision.outcome is EvaluationOutcome.MATCHED
    assert decision.route is CanonicalRoute.NO_ACTION
    assert decision.reason_codes == ["internal_distribution_list"]


def test_removed_group_addresses_rejoin_normal_routing():
    """The 2026-08-10 owner review pulled m.wu/shj-zhen/yq.w/zhang-xia out of
    the silent-group list: an email merely addressed to one of them is no
    longer no_action and, with no other rule applying, falls through to
    Tier 2/3."""
    artifact = _compile()
    rules = _rules(artifact)
    for removed in (
        "m.wu@tianjin-air.com",
        "shj-zhen@tianjin-air.com",
        "yq.w@tianjin-air.com",
        "zhang-xia@tianjin-air.com",
    ):
        view = EmailView(sender_address="ops@hnair.com", to_addresses=[removed])
        decision = build_tier1_decision(rules, view, me_email=ME_EMAIL)
        assert decision.outcome is EvaluationOutcome.ABSTAIN, removed
        assert decision.route is None, removed


def test_vip_direct_recipient_is_reply_not_read_only():
    artifact = _compile()
    view = EmailView(sender_address="lanjuan@tianjin-air.com", to_addresses=[ME_EMAIL])
    decision = build_tier1_decision(_rules(artifact), view, me_email=ME_EMAIL)
    assert decision.outcome is EvaluationOutcome.MATCHED
    assert decision.route is CanonicalRoute.REPLY
    assert decision.decision_origin is DecisionOrigin.RULE_DECLARED


def test_vip_cc_only_is_read_only_not_reply():
    artifact = _compile()
    view = EmailView(
        sender_address="lanjuan@tianjin-air.com",
        to_addresses=["someone-else@tianjin-air.com"],
        cc_addresses=[ME_EMAIL],
    )
    decision = build_tier1_decision(_rules(artifact), view, me_email=ME_EMAIL)
    assert decision.outcome is EvaluationOutcome.MATCHED
    assert decision.route is CanonicalRoute.READ_ONLY


def test_vip_split_is_mutually_exclusive_never_a_runtime_conflict():
    """The two VIP rules share an anchor address (static-overlap warning at
    compile time) but must never both MATCH the same email at runtime,
    because their conditions are the $ME predicate and its negation."""
    artifact = _compile()
    rules = _rules(artifact)
    for to_addresses, cc_addresses in (
        ([ME_EMAIL], []),
        (["someone-else@tianjin-air.com"], [ME_EMAIL]),
        (["someone-else@tianjin-air.com"], []),
    ):
        view = EmailView(
            sender_address="xt_zong@tianjin-air.com",
            to_addresses=to_addresses,
            cc_addresses=cc_addresses,
        )
        decision = build_tier1_decision(rules, view, me_email=ME_EMAIL)
        assert decision.outcome is not EvaluationOutcome.CONFLICT


def test_non_vip_direct_recipient_is_read_only():
    """Counterpart of T1-VIP-DIRECT-REPLY-001: a direct recipient email from
    any sender without a more specific rule is read_only, not a Tier 2/3
    abstention."""
    artifact = _compile()
    rules = _rules(artifact)
    for sender in ("some-ops@hnair.com", "colleague@tianjin-air.com"):
        view = EmailView(sender_address=sender, to_addresses=[ME_EMAIL])
        decision = build_tier1_decision(rules, view, me_email=ME_EMAIL)
        assert decision.outcome is EvaluationOutcome.MATCHED, sender
        assert decision.route is CanonicalRoute.READ_ONLY, sender
        assert decision.business_flow_ids == ["direct-recipient-fyi"], sender


def test_non_vip_fallback_never_shadows_sender_specific_rules():
    """The fallback excludes senders owned by other rules; otherwise hhsc/
    hnasafety direct-to-owner mail would conflict (no_action vs read_only)
    instead of archiving. Asserting the exact route below doubles as the
    exclusion regression check."""
    artifact = _compile()
    rules = _rules(artifact)
    for sender, reason in (
        ("hnasafety@hnaaviation.com", "automated_system_notification"),
        ("hhsc@hnair.com", "marketing_spam"),
    ):
        view = EmailView(sender_address=sender, to_addresses=[ME_EMAIL])
        decision = build_tier1_decision(rules, view, me_email=ME_EMAIL)
        assert decision.outcome is EvaluationOutcome.MATCHED, sender
        assert decision.route is CanonicalRoute.NO_ACTION, sender
        assert decision.reason_codes == [reason], sender


def test_safety_platform_and_marketing_are_distinct_no_action_reasons():
    artifact = _compile()
    rules = _rules(artifact)

    safety_view = EmailView(sender_address="hnasafety@hnaaviation.com", to_addresses=[ME_EMAIL])
    safety_decision = build_tier1_decision(rules, safety_view, me_email=ME_EMAIL)
    assert safety_decision.route is CanonicalRoute.NO_ACTION
    assert safety_decision.reason_codes == ["automated_system_notification"]

    marketing_view = EmailView(sender_address="hhsc@hnair.com", to_addresses=[ME_EMAIL])
    marketing_decision = build_tier1_decision(rules, marketing_view, me_email=ME_EMAIL)
    assert marketing_decision.route is CanonicalRoute.NO_ACTION
    assert marketing_decision.reason_codes == ["marketing_spam"]


def test_zhangxia_forward_requires_both_sender_and_subject_shape():
    artifact = _compile()
    rules = _rules(artifact)

    for sender in ("zhang-xia@tianjin-air.com", "m.wu@tianjin-air.com"):
        matches = EmailView(sender_address=sender, subject="Fw: 通知")
        decision = build_tier1_decision(rules, matches, me_email=ME_EMAIL)
        assert decision.route is CanonicalRoute.NO_ACTION, sender
        assert decision.reason_codes == ["forwarded_informational"], sender

        not_a_forward = EmailView(sender_address=sender, subject="请审批")
        assert build_tier1_decision(rules, not_a_forward, me_email=ME_EMAIL).outcome is EvaluationOutcome.ABSTAIN, sender

    wrong_sender = EmailView(sender_address="someone-else@tianjin-air.com", subject="Fw: 通知")
    assert build_tier1_decision(rules, wrong_sender, me_email=ME_EMAIL).outcome is EvaluationOutcome.ABSTAIN


def test_distlist_read_only_groups_are_read_only():
    """The 2026-08-12 owner review moved thxxcxb/thxxyfzx out of the silent
    group list into T1-INTERNAL-DISTLIST-READONLY-001: mail to these
    department/center groups must still be read (read_only FYI), not
    no_action."""
    artifact = _compile()
    rules = _rules(artifact)
    for group in ("thxxcxb@hnair.com", "thxxyfzx@hnair.com"):
        view = EmailView(sender_address="ops@hnair.com", to_addresses=[group])
        decision = build_tier1_decision(rules, view, me_email=ME_EMAIL)
        assert decision.outcome is EvaluationOutcome.MATCHED, group
        assert decision.route is CanonicalRoute.READ_ONLY, group
        assert decision.business_flow_ids == ["internal-distribution-list-fyi"], group


def test_distlist_read_only_excludes_zhangxia_and_mwu_forwards():
    """Forwarded group mail remains owned by the dedicated no_action rule."""
    artifact = _compile()
    rules = _rules(artifact)

    for sender, subject, group in (
        (
            "zhang-xia@tianjin-air.com",
            "转发: 部门通知",
            "thxxcxb@hnair.com",
        ),
        (
            "m.wu@tianjin-air.com",
            "Fw: 部门通知",
            "thxxyfzx@hnair.com",
        ),
    ):
        decision = build_tier1_decision(
            rules,
            EmailView(
                sender_address=sender,
                to_addresses=[group],
                subject=subject,
            ),
            me_email=ME_EMAIL,
        )
        assert decision.outcome is EvaluationOutcome.MATCHED, sender
        assert decision.route is CanonicalRoute.NO_ACTION, sender
        assert decision.reason_codes == ["forwarded_informational"], sender
        assert [
            item.rule_id for item in decision.matched_rules
        ] == ["T1-ZHANGXIA-FORWARD-READONLY-001"], sender

    ordinary = build_tier1_decision(
        rules,
        EmailView(
            sender_address="zhang-xia@tianjin-air.com",
            to_addresses=["thxxcxb@hnair.com"],
            subject="请阅：部门通知",
        ),
        me_email=ME_EMAIL,
    )
    assert ordinary.outcome is EvaluationOutcome.MATCHED
    assert ordinary.route is CanonicalRoute.READ_ONLY
    assert ordinary.business_flow_ids == ["internal-distribution-list-fyi"]


def test_zhangxia_forward_to_silent_distlist_merges_same_no_action():
    artifact = _compile()
    view = EmailView(
        sender_address="zhang-xia@tianjin-air.com",
        to_addresses=["gs4193@hnair.com"],
        subject="转发: 请阅处：关于修订《安全管理手册》《安全管理程序手册》的会签",
    )

    decision = build_tier1_decision(_rules(artifact), view, me_email=ME_EMAIL)

    assert decision.outcome is EvaluationOutcome.MATCHED
    assert decision.route is CanonicalRoute.NO_ACTION
    assert len(decision.candidate_actions) == 1
    assert sorted(decision.candidate_actions[0].rule_ids) == [
        "T1-INTERNAL-DISTLIST-SILENCE-001",
        "T1-ZHANGXIA-FORWARD-READONLY-001",
    ]
    assert decision.reason_codes == [
        "forwarded_informational",
        "internal_distribution_list",
    ]


def test_direct_to_me_plus_distlist_is_conflict_manual_review():
    """By-design corner: mail addressed to both $ME and a silent distlist
    matches two different actions (read_only fallback vs no_action group) and
    is forced to manual review rather than silently picking one."""
    artifact = _compile()
    view = EmailView(
        sender_address="ops@hnair.com",
        to_addresses=[ME_EMAIL, "tjhkgh@tianjin-air.com"],
    )
    decision = build_tier1_decision(_rules(artifact), view, me_email=ME_EMAIL)
    assert decision.outcome is EvaluationOutcome.CONFLICT
    assert decision.route is CanonicalRoute.MANUAL_REVIEW


def test_non_direct_email_abstains_falls_through_to_tier2_tier3():
    """The non-VIP fallback only claims direct (To) email: cc-only or
    not-for-the-owner mail still falls through to Tier 2/3."""
    artifact = _compile()
    rules = _rules(artifact)

    cc_only = EmailView(
        sender_address="rando@external.com",
        to_addresses=["someone-else@tianjin-air.com"],
        cc_addresses=[ME_EMAIL],
    )
    assert build_tier1_decision(rules, cc_only, me_email=ME_EMAIL).outcome is EvaluationOutcome.ABSTAIN

    not_mine = EmailView(sender_address="rando@external.com", to_addresses=["colleague@tianjin-air.com"])
    assert build_tier1_decision(rules, not_mine, me_email=ME_EMAIL).outcome is EvaluationOutcome.ABSTAIN
