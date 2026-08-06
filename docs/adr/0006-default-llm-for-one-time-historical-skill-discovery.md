---
status: accepted
---

# Use the configured model by default for one-time historical skill discovery

A manually initiated Historical Skill Discovery run uses the configured LLM by default, without an additional per-run authorization step. It may analyze bounded historical mail metadata and text samples to infer candidates, but never enables a rule; the operator still selects candidates in conversation before promotion. Heuristic-only discovery remains available when wanted.
