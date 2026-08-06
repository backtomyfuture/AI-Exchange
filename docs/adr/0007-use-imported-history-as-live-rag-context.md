---
status: accepted
---

# Use imported history as live RAG context

Manually imported Historical Email remains in the shared Qdrant email corpus and is eligible for retrieval while processing a new Inbound Email, as well as for the one-time Historical Skill Discovery. A separate discovery-only corpus would duplicate the knowledge base and discard useful precedent when drafting or classifying current mail.
