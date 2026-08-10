---
status: accepted
---

# Manual Rule Drafts are decoupled from Skill Promotion

The Operations Console becomes the primary way new Declarative Tier 1 Skills get
authored going forward, because in practice new rules mostly come from an operator
noticing a specific new case (a sender, a recipient, a business flow) rather than from
mining Historical Email in bulk. It does not take over Candidate Review or Skill
Promotion: `scripts/discover_skills.py`'s discovery/promotion workflow remains a
separate, occasional tool for bulk historical pattern mining, and a Discovered Skill
Candidate still only reaches `tier1_rules/` through its own conversational review and
promotion, never through the console. A Rule Draft authored directly in the console and
a promoted Discovered Skill Candidate both end up as ordinary YAML files in
`tier1_rules/` and both wait for the same planned-restart Skill Activation — the two
paths only differ in where the rule's content originates and how it is reviewed before
being written. We chose to keep them separate rather than folding candidate review into
the console because they solve different problems (ad hoc authoring vs. bulk discovery)
and merging them would couple the console's build to a workflow that, per this decision,
is no longer expected to be in regular use.
