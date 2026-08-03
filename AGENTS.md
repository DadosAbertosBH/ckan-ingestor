# Project Rules

## Communication

Avoid negative ontologies on executions and/or explanations — frame actions positively (what WILL be done), not negatively (what won't/can't).

## Repositories

- **ckan-ingestor** (this repo): Application source code, Helm chart
- **argocd-applications** (`noctcloud/argocd-applications`): GitOps deployment config — ArgoCD Application, ExternalSecret, Crossplane resources. Local copy at `./argocd-applications/`.

## TDD — Always

Every implementation, bug fix, or adjustment must follow TDD:

1. **RED** — write a failing test that reproduces the bug or validates the expected behavior
2. **GREEN** — apply the minimal fix to make the test pass
3. Run the full suite:
   - `cd /Users/guilhermecastro/Projects/ckan-ingestor && .venv/bin/python -m pytest tests/ -q`
   - `cd /Users/guilhermecastro/Projects/ckan-ingestor/ingestor_orchestrator/backend && .venv/bin/python -m pytest tests/ -q`

Never jump straight to code without a test first. If you do, revert and start with the test.

## GitOps — Never patch directly

ArgoCD-managed resources must only be changed via Git, never with `kubectl patch`/`kubectl edit`. The flow is always:

1. Edit the manifest in the appropriate repo (`argocd-applications` or `ckan-ingestor`)
2. Commit and push

## Lint before committing

Before making any commit or suggesting a commit with Python file changes, run:

```bash
uv run ruff check
```

If there are errors:
- Fix auto-fixable issues with `uv run ruff check --fix`
- Manually fix any remaining issues
- Re-run to confirm zero errors

## License before committing

Before making any commit or suggesting a commit, run:

```bash
./add_license.sh
```

This ensures all `.py` files have the AGPL license header.
