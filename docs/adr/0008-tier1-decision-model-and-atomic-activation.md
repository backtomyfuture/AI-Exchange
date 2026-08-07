---
status: accepted
---

# Tier 1 decision model and atomic ruleset activation

The full schema, DSL, activation pipeline, audit fields, and rule-migration taxonomy are
specified in `docs/tier1-routing-design.md`; this record captures the binding decisions.
Tier 1 routing output is split into an immutable `tier1_decision` (`EvaluationOutcome`:
matched/abstain/conflict/error; `CanonicalRoute`: reply/forward/read_only/no_action/
manual_review, replacing the ambiguous `skip`) and a separately mutable
`handoff_execution` state machine; a downstream execution failure only changes
`handoff_execution.state`, never triggers Tier 2/3 reclassification or rewrites the
persisted decision. Multiple matching rules are merged only when their
`action_fingerprint` — a versioned hash of the canonicalized `route` and its
default-expanded `params`, deliberately excluding the non-authoritative
`business_flow_id` audit label — is identical; any distinct fingerprint produces
`outcome=conflict, route=manual_review` with every competing fingerprint retained in
`candidate_actions`, never a single overwritten value. `priority`/`intent` remain
non-authoritative descriptive metadata (`governance.criticality`, still `P0`–`P3`) and
must never select a winner. Declarative field-match conditions use three-valued logic
(`VALUE`/`EMPTY`/`UNKNOWN`) rather than treating a merely absent field as unmatched;
an anchor evaluating to `UNKNOWN`, or a matched anchor whose content condition is
`UNKNOWN`, produces `outcome=error, route=manual_review` rather than a silent
abstain. The production `enabled` ruleset is compiled and validated as one atomic
unit — schema, address/group reference, regex compilation, route-params, duplicate-ID,
static overlap, and fixture replay — into an immutable, digest-addressed registry
artifact; any single failing rule blocks that artifact's creation and activation while
the currently running artifact and process are unaffected, and only a failure to load
an already-selected artifact is fail-hard, with no in-process silent fallback to an
older revision. This chooses full-batch validation over the previously considered
per-rule isolation because a partially loaded ruleset would run in production without
ever having been validated as a whole. `status` (`proposed`/`enabled`/`retired`) is
hand-edited in YAML and never rewritten by the system; an independent
`validity.effective_from`/`expires_at` window governs runtime matching, so an expired
rule simply stops matching and raises a persisted alert instead of the system silently
flipping `status`. `no_action` and rule-declared `manual_review` carry a
business-namespaced `reason_code`, kept separate from the existing system
`MANUAL_REVIEW_CODES`. Validation records are the `governance.positive_cases`/
`negative_cases` fixtures committed alongside each rule and replayed on every atomic
activation, plus the normal Git/PR review of that YAML change — no new database
approval-record table. This version does not introduce a directory-backed identity
resolver, an Intake Guard detection layer, a card drill-down viewer, or a new
per-message routing-audit table; those remain explicitly deferred, and the 31 existing
`skills_registry` candidates must be re-reviewed under the migration taxonomy in
`docs/tier1-routing-design.md` rather than auto-migrated.
