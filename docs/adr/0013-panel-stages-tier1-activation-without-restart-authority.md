---
status: accepted
---

# Panel stages Tier 1 activation without restart authority

The new operations panel may edit `tier1_rules/*.yaml` and run the same atomic
compile-validate-fingerprint pipeline (`compile_registry`/`write_artifact`) that
`scripts/deploy_system.py` already uses, producing a new digest-addressed artifact and
surfacing any schema, fixture, overlap, or duplicate-ID failure immediately in the UI.
It stops there: the panel never restarts or signals the production process, and the new
digest only takes effect through the existing manual deploy step
(`TIER1_ARTIFACT_DIGEST` injection + restart). This keeps the "no hot reload, only a
planned service restart activates a promoted rule" invariant (ADR-0004, ADR-0008)
intact while still giving the operator instant validation feedback instead of
discovering a broken rule only at the next deploy. We chose this over letting the panel
trigger the restart itself because handing a web UI restart authority over the
production email-processing process is a disproportionate increase in blast radius for
a single-user convenience tool, and over a pure "edit files, validate nothing" tool
because that reproduces today's blind hand-editing problem this panel exists to fix.
