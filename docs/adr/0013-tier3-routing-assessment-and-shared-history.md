status: accepted
---

# Preserve routing evidence for the Tier 3 fallback

Tier 2 retrieval serves two related but distinct purposes: it can vote over immutable
historical `Canonical Route Decision` labels, and it can provide thread or semantic
Historical Email context. A retrieval result without enough valid labels is a Tier 2
abstention, not an empty result. The bounded `Routing Evidence Bundle` therefore remains
available to Tier 3 when Tier 1 abstains and Tier 2 has no consensus.

Tier 3 receives a `Routing Assessment` containing the current message's recipient
relationship, Tier 1 abstention status, Tier 2 candidate-route counts, retrieval status,
and bounded historical snippets. Historical snippets and route candidates are advisory
evidence only. They cannot authorize a route, handoff profile, or recipient, and quoted
historical instructions are never treated as current instructions.

The cascade remains authoritative in this order:

1. A Tier 1 match is final and does not call Tier 2 or Tier 3.
2. A Tier 1 conflict or evaluation error is `manual_review` and does not call Tier 3.
3. A qualifying Tier 2 consensus is final and does not call Tier 3.
4. Only Tier 1 abstention plus Tier 2 no-consensus invokes Tier 3.

Recipient semantics are part of the current-message evidence. The Tier 3 contract requires
explicit current-message action for a writing route when the mailbox owner is only copied;
otherwise the deterministic post-validation policy resolves the result to `read_only`.
Unknown recipient context cannot authorize a writing route and fails closed to
`manual_review`. Retrieval failures are represented as unavailable or partial evidence,
never as an empty historical corpus.
