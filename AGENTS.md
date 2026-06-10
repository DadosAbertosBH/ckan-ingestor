# Project Rules

## TDD — Always

Every implementation, bug fix, or adjustment must follow TDD:

1. **RED** — write a failing test that reproduces the bug or validates the expected behavior
2. **GREEN** — apply the minimal fix to make the test pass
3. Run the full suite:
   - `cd /Users/guilhermecastro/Projects/ckan-ingestor && .venv/bin/python -m pytest tests/ -q`
   - `cd /Users/guilhermecastro/Projects/ckan-ingestor/ingestor_orchestrator/backend && .venv/bin/python -m pytest tests/ -q`

Never jump straight to code without a test first. If you do, revert and start with the test.

## Lint before committing

Before making any commit or suggesting a commit with Python file changes, run:

```bash
uv run ruff check
```

If there are errors:
- Fix auto-fixable issues with `uv run ruff check --fix`
- Manually fix any remaining issues
- Re-run to confirm zero errors
