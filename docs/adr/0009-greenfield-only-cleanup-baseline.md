---
status: accepted
---

# Support only Greenfield Deployment after this cleanup

This cleanup targets a new, empty AI Email Assistance installation and deliberately does not preserve in-place PostgreSQL or runtime-state upgrades. The existing deployment data has no business value, so deleting obsolete compatibility implementation, migrations, relations, constraints, and role grants is preferable to carrying them into the clean baseline; fresh bootstrap and runtime verification remain required before any old local resources are discarded. After that verification, the prior Compose project's containers and named volumes may be removed, while local secrets remain retained as a credential backup.
