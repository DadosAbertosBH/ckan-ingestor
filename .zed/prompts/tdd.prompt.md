---
title: "Always use TDD"
description: "Every implementation, fix, or adjustment must start with a failing test."
---

# Rule: Always use TDD

**This rule must be followed in ALL code changes, regardless of context.**

Every implementation, bug fix, or adjustment must follow TDD:

1. **RED** — write a failing test that reproduces the bug or validates the expected behavior
2. **GREEN** — apply the minimal fix to make the test pass
3. Run the full suite to ensure nothing is broken:
   - Root: `cd /Users/guilhermecastro/Projects/ckan-ingestor && .venv/bin/python -m pytest tests/ -q`
   - Backend: `cd /Users/guilhermecastro/Projects/ckan-ingestor/ingestor_orchestrator/backend && .venv/bin/python -m pytest tests/ -q`

Never jump straight to code. If you do, revert and start with the test.

Tests go in the nearest test directory:
- Backend: `ingestor_orchestrator/backend/tests/`
- Ingestor: `tests/ingestors/`
- Root: `tests/`
