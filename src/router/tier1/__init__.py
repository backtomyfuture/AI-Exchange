"""Tier 1 v1 declarative routing (see ``docs/tier1-routing-design.md``).

Standalone from ``src/router/base.py``/``manager.py``/``tier1_reflex.py``/
``engine.py``/``auto_skill.py``, which remain the live production path until the
31 frozen ``skills_registry/`` candidates are individually migrated under the
taxonomy in the design doc (§10). Modules here are imported directly by callers
(``schema``, ``dsl``, ``fingerprint``, ``decision``, ``compiler``); this package
intentionally does not re-export them.
"""
