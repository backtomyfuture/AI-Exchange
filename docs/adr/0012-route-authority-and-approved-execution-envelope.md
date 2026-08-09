---
status: accepted
---

# Make the durable route and approved envelope the only execution authorities

The workflow uses one immutable `RouteDecision` as its control plane. Tier 1 may match
deterministically; otherwise Historical Route Consensus may vote over distinct immutable
historical labels; Tier 3 is the final model fallback. Abstention is an intermediate tier
result and cannot be persisted as a final decision. The selected decision is inserted once
and exact-read-back under the live inbox lease fence before handoff profiles, evidence
adapters, Qdrant, other model calls, LangGraph, or user-visible effects. Recovery reuses that
decision only after revalidating generation, fencing token, execution and authority epochs,
capability, lease session and owner, and email identity/version. Downstream failure changes
only the durable handoff disposition and can require manual review; it never reopens routing.

Writing routes select a versioned, read-only `HandoffProfile`. The profile expands into a
digest-addressed `HandoffPlan`; a closed registry of typed read-only adapters creates a
separate `EvidencePack`. Every adapter effect is authorized against the same live lease.
Historical Route Consensus and writing evidence retrieval are distinct modules: evidence
may support drafting and review but cannot encode route, profile, or recipients. The first
business profile is `vip_direct_reply_v1`, which combines its bounded writing instruction
with optional Exchange directory, mail-thread, and semantic-history evidence.

Approval freezes an append-only `execution_payload_revisions` row containing the exact
decision, plan and evidence digests, draft, To/Cc, and attachment manifest. Card actions bind
both revision and digest; editing any field appends a new revision, making an older action
stale. Approval transactionally validates that binding and creates an append-only
`ApprovedExecutionEnvelope`. The sender reads only this envelope and its stored digest,
passes it through a deterministic `ExecutionGate`, then claims execution before calling
Exchange. Mutable checkpoint fields and legacy card state are not send authority. A gate
failure can block and move the handoff to manual review but cannot alter approved content or
reroute it. The pre-approval reviewer remains a Draft Quality Gate, not a fourth routing tier.

Before business routing, a conservative `IntakeGuard` records pass, suppress, or quarantine
for each execution epoch. Automatic replies and explicit loops may be suppressed; NDR,
sensitivity/confidentiality markers, and malformed details are quarantined. Fuzzy spam or
marketing classification is excluded. Tier 0 suppress and Tier 1 no_action remain separate
auditable concepts, and releasing quarantine appends a release record and starts a newer
execution epoch instead of mutating the original decision.

Because this is a Greenfield Deployment, these contracts live in the single bootstrap
baseline (`intake_decisions`, `intake_releases`, `tier1_decisions`, `handoff_runs`,
`execution_payload_revisions`, and `approved_execution_envelopes`) rather than an incremental
migration. Immutable artifacts are protected from update, delete, and truncate; mutable
handoff state advances through versioned compare-and-swap transitions.
