# TDD — Always

Every implementation, fix, or adjustment must follow TDD:

1. **RED** — write a failing test that reproduces the bug or validates the expected behavior
2. **GREEN** — apply the minimal fix to make the test pass
3. Run the full suite to ensure nothing is broken (`tests/` + `ingestor_orchestrator/backend/tests/`)

Never skip straight to the code without a test first.
