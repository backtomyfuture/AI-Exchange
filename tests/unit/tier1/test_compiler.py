"""Tier 1 v1 registry compiler tests (docs/tier1-routing-design.md §6, §7)."""
import json
import textwrap

from src.router.tier1.compiler import (
    CompilationFailure,
    CompiledArtifact,
    compile_registry,
    write_artifact,
)


def _write(tmp_path, name, content):
    (tmp_path / name).write_text(textwrap.dedent(content))


def test_compiles_valid_ruleset_to_artifact(tmp_path):
    _write(
        tmp_path,
        "r1.yaml",
        """
        rule_id: RULE-OK-001
        rule_version: 1
        status: enabled
        owner: team-x
        match:
          anchor: {any: [{field: sender.address, op: eq, value: a@example.com}]}
        decision: {route: read_only}
        governance:
          positive_cases: [{case_id: p1, email: {sender: {address: a@example.com}}}]
        """,
    )
    result = compile_registry(tmp_path)
    assert isinstance(result, CompiledArtifact)
    assert [r.manifest.rule_id for r in result.rules] == ["RULE-OK-001"]
    assert result.warnings == []


def test_write_artifact_switches_pointer_atomically(tmp_path):
    _write(
        tmp_path,
        "r1.yaml",
        """
        rule_id: RULE-OK-002
        rule_version: 1
        status: enabled
        owner: team-x
        match:
          anchor: {any: [{field: sender.address, op: eq, value: a@example.com}]}
        decision: {route: read_only}
        governance:
          positive_cases: [{case_id: p1, email: {sender: {address: a@example.com}}}]
        """,
    )
    artifact = compile_registry(tmp_path)
    out_dir = tmp_path / "artifacts"
    path = write_artifact(artifact, out_dir)
    assert path.exists()
    pointer = json.loads((out_dir / "current.json").read_text())
    assert pointer["digest"] == artifact.digest
    assert pointer["path"] == path.name


def test_duplicate_rule_id_is_hard_error(tmp_path):
    rule_yaml = """
    rule_id: RULE-DUP-001
    rule_version: 1
    status: enabled
    owner: team-x
    match:
      anchor: {{any: [{{field: sender.address, op: eq, value: {addr}}}]}}
    decision: {{route: read_only}}
    governance:
      positive_cases: [{{case_id: p1, email: {{sender: {{address: {addr}}}}}}}]
    """
    _write(tmp_path, "r1.yaml", rule_yaml.format(addr="a@example.com"))
    _write(tmp_path, "r2.yaml", rule_yaml.format(addr="b@example.com"))
    result = compile_registry(tmp_path)
    assert isinstance(result, CompilationFailure)
    assert any(e.code == "duplicate_rule_id" for e in result.errors)


def test_enabled_rule_schema_failure_blocks_entire_artifact(tmp_path):
    _write(
        tmp_path,
        "good.yaml",
        """
        rule_id: RULE-GOOD-001
        rule_version: 1
        status: enabled
        owner: team-x
        match:
          anchor: {any: [{field: sender.address, op: eq, value: a@example.com}]}
        decision: {route: read_only}
        governance:
          positive_cases: [{case_id: p1, email: {sender: {address: a@example.com}}}]
        """,
    )
    _write(
        tmp_path,
        "broken_enabled.yaml",
        """
        rule_id: RULE-BROKEN-001
        rule_version: 1
        status: enabled
        match:
          anchor: {any: [{field: sender.address, op: contains, value: a@example.com}]}
        decision: {route: read_only}
        """,
    )
    result = compile_registry(tmp_path)
    assert isinstance(result, CompilationFailure)
    assert any(e.code == "schema_invalid" for e in result.errors)


def test_broken_proposed_rule_does_not_block_activation(tmp_path):
    _write(
        tmp_path,
        "good.yaml",
        """
        rule_id: RULE-GOOD-002
        rule_version: 1
        status: enabled
        owner: team-x
        match:
          anchor: {any: [{field: sender.address, op: eq, value: a@example.com}]}
        decision: {route: read_only}
        governance:
          positive_cases: [{case_id: p1, email: {sender: {address: a@example.com}}}]
        """,
    )
    _write(
        tmp_path,
        "broken_proposed.yaml",
        """
        rule_id: RULE-BROKEN-002
        rule_version: 1
        status: proposed
        match:
          anchor: {any: [{field: sender.address, op: contains, value: a@example.com}]}
        decision: {route: read_only}
        """,
    )
    result = compile_registry(tmp_path)
    assert isinstance(result, CompiledArtifact)
    assert [r.manifest.rule_id for r in result.rules] == ["RULE-GOOD-002"]
    assert any(w.code == "schema_invalid_non_enabled" for w in result.warnings)


def test_unsafe_regex_is_rejected_without_crashing(tmp_path):
    _write(
        tmp_path,
        "r1.yaml",
        r"""
        rule_id: RULE-REGEX-001
        rule_version: 1
        status: enabled
        owner: team-x
        match:
          anchor: {any: [{field: sender.address, op: eq, value: a@example.com}]}
          conditions: {all: [{field: subject, op: regex, value: '(a+)+$'}]}
        decision: {route: read_only}
        governance:
          positive_cases: [{case_id: p1, email: {sender: {address: a@example.com}, subject: aaa}}]
          negative_cases: [{case_id: n1, email: {sender: {address: a@example.com}, subject: bbb}}]
        """,
    )
    result = compile_registry(tmp_path)
    assert isinstance(result, CompilationFailure)
    assert any(e.code == "unsafe_regex" for e in result.errors)


def test_external_forward_recipient_without_acknowledgement_is_hard_error(tmp_path):
    _write(
        tmp_path,
        "r1.yaml",
        """
        rule_id: RULE-EXT-001
        rule_version: 1
        status: enabled
        owner: team-x
        match:
          anchor: {any: [{field: sender.address, op: eq, value: a@example.com}]}
        decision:
          route: forward
          params: {fixed_recipients: [outside@external.com]}
        governance:
          positive_cases: [{case_id: p1, email: {sender: {address: a@example.com}}}]
        """,
    )
    result = compile_registry(tmp_path, internal_email_domains=["example.com"])
    assert isinstance(result, CompilationFailure)
    assert any(e.code == "external_recipient_not_acknowledged" for e in result.errors)


def test_external_forward_recipient_with_acknowledgement_compiles(tmp_path):
    _write(
        tmp_path,
        "r1.yaml",
        """
        rule_id: RULE-EXT-002
        rule_version: 1
        status: enabled
        owner: team-x
        match:
          anchor: {any: [{field: sender.address, op: eq, value: a@example.com}]}
        decision:
          route: forward
          params: {fixed_recipients: [outside@external.com]}
        governance:
          positive_cases: [{case_id: p1, email: {sender: {address: a@example.com}}}]
          external_recipient_acknowledged: true
        """,
    )
    result = compile_registry(tmp_path, internal_email_domains=["example.com"])
    assert isinstance(result, CompiledArtifact)


def test_fixture_positive_case_mismatch_is_hard_error(tmp_path):
    _write(
        tmp_path,
        "r1.yaml",
        """
        rule_id: RULE-FIX-001
        rule_version: 1
        status: enabled
        owner: team-x
        match:
          anchor: {any: [{field: sender.address, op: eq, value: a@example.com}]}
        decision: {route: read_only}
        governance:
          positive_cases: [{case_id: p1, email: {sender: {address: wrong@example.com}}}]
        """,
    )
    result = compile_registry(tmp_path)
    assert isinstance(result, CompilationFailure)
    assert any(e.code == "fixture_positive_failed" for e in result.errors)


def test_fixture_negative_case_mismatch_is_hard_error(tmp_path):
    _write(
        tmp_path,
        "r1.yaml",
        """
        rule_id: RULE-FIX-002
        rule_version: 1
        status: enabled
        owner: team-x
        match:
          anchor: {any: [{field: sender.address, op: eq, value: a@example.com}]}
          conditions: {all: [{field: subject, op: contains, value: refund}]}
        decision: {route: read_only}
        governance:
          positive_cases: [{case_id: p1, email: {sender: {address: a@example.com}, subject: refund now}}]
          negative_cases: [{case_id: n1, email: {sender: {address: a@example.com}, subject: refund too}}]
        """,
    )
    result = compile_registry(tmp_path)
    assert isinstance(result, CompilationFailure)
    assert any(e.code == "fixture_negative_failed" for e in result.errors)


def test_identical_normalized_match_with_divergent_action_is_hard_error(tmp_path):
    _write(
        tmp_path,
        "r1.yaml",
        """
        rule_id: RULE-OVERLAP-001
        rule_version: 1
        status: enabled
        owner: team-x
        match:
          anchor: {any: [{field: sender.address, op: eq, value: a@example.com}]}
        decision: {route: read_only}
        governance:
          positive_cases: [{case_id: p1, email: {sender: {address: a@example.com}}}]
        """,
    )
    _write(
        tmp_path,
        "r2.yaml",
        """
        rule_id: RULE-OVERLAP-002
        rule_version: 1
        status: enabled
        owner: team-x
        match:
          anchor: {any: [{field: sender.address, op: eq, value: A@Example.com}]}
        decision: {route: no_action, params: {reason_code: dup_test}}
        validity: {expires_at: '2099-01-01'}
        governance:
          positive_cases: [{case_id: p1, email: {sender: {address: a@example.com}}}]
          negative_cases:
            - {case_id: n1, email: {sender: {address: z@example.com}}}
            - {case_id: n2, email: {sender: {address: y@example.com}}}
        """,
    )
    result = compile_registry(tmp_path)
    assert isinstance(result, CompilationFailure)
    assert any(e.code == "duplicate_match_divergent_action" for e in result.errors)


def test_shared_anchor_address_with_different_action_is_warning_not_error(tmp_path):
    """Different (non-identical) matches that happen to share an anchor address
    are a *possible* overlap warning, not a hard block — runtime conflict
    handling is the final authority for genuinely overlapping emails."""
    _write(
        tmp_path,
        "r1.yaml",
        """
        rule_id: RULE-WARN-001
        rule_version: 1
        status: enabled
        owner: team-x
        match:
          anchor: {any: [{field: sender.address, op: eq, value: a@example.com}]}
        decision: {route: read_only}
        governance:
          positive_cases: [{case_id: p1, email: {sender: {address: a@example.com}}}]
        """,
    )
    _write(
        tmp_path,
        "r2.yaml",
        """
        rule_id: RULE-WARN-002
        rule_version: 1
        status: enabled
        owner: team-x
        match:
          anchor: {any: [{field: sender.address, op: eq, value: a@example.com}]}
          conditions: {all: [{field: subject, op: contains, value: refund}]}
        decision: {route: forward, params: {fixed_recipients: [ops@example.com]}}
        governance:
          positive_cases: [{case_id: p1, email: {sender: {address: a@example.com}, subject: refund}}]
          negative_cases: [{case_id: n1, email: {sender: {address: a@example.com}, subject: other}}]
        """,
    )
    result = compile_registry(tmp_path, internal_email_domains=["example.com"])
    assert isinstance(result, CompiledArtifact)
    assert any(w.code == "possible_anchor_overlap" for w in result.warnings)


def test_me_placeholder_fixture_replay_uses_supplied_me_email(tmp_path):
    _write(
        tmp_path,
        "r1.yaml",
        """
        rule_id: RULE-ME-001
        rule_version: 1
        status: enabled
        owner: team-x
        match:
          anchor: {any: [{field: sender.address, op: eq, value: vip@example.com}]}
          conditions: {all: [{field: to.addresses, op: has_any, values: ['$ME']}]}
        decision: {route: reply, params: {reply_mode: sender_and_original_cc}}
        governance:
          positive_cases:
            - {case_id: p1, email: {sender: {address: vip@example.com}, to: [me@example.com]}}
          negative_cases:
            - {case_id: n1, email: {sender: {address: vip@example.com}, to: [other@example.com]}}
        """,
    )
    result = compile_registry(tmp_path, me_email="me@example.com")
    assert isinstance(result, CompiledArtifact)


def test_me_placeholder_fixture_replay_fails_without_me_email(tmp_path):
    """Without me_email, the $ME leaf resolves to UNKNOWN, the positive case
    can never MATCH, and compilation must fail loudly rather than silently
    activate a rule that can never fire as intended."""
    _write(
        tmp_path,
        "r1.yaml",
        """
        rule_id: RULE-ME-002
        rule_version: 1
        status: enabled
        owner: team-x
        match:
          anchor: {any: [{field: sender.address, op: eq, value: vip@example.com}]}
          conditions: {all: [{field: to.addresses, op: has_any, values: ['$ME']}]}
        decision: {route: reply, params: {reply_mode: sender_and_original_cc}}
        governance:
          positive_cases:
            - {case_id: p1, email: {sender: {address: vip@example.com}, to: [me@example.com]}}
          negative_cases:
            - {case_id: n1, email: {sender: {address: vip@example.com}, to: [other@example.com]}}
        """,
    )
    result = compile_registry(tmp_path)
    assert isinstance(result, CompilationFailure)
    assert any(e.code == "fixture_positive_failed" for e in result.errors)
