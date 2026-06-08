---
title: "Lint before committing"
description: "Always run ruff check before committing or suggesting commits."
---

# Rule: Always run lint before committing

**This rule must be followed in ALL interactions, regardless of context.**

Before making any commit or suggesting a commit with Python file changes, run:

```bash
uv run ruff check
```

If there are errors:
- Fix auto-fixable issues with `uv run ruff check --fix`
- Manually fix any remaining issues
- Re-run `uv run ruff check` to confirm zero errors

The project's CI runs `uv run ruff check` in the `test` stage and **will fail** if lint errors are present.
