# Project Rules

## Communication

Avoid negative ontologies on executions and/or explanations — frame actions positively (what WILL be done), not negatively (what won't/can't).

## Repositories

- **ckan-ingestor** (this repo): Application source code, Helm chart
- **argocd-applications** (`noctcloud/argocd-applications`): GitOps deployment config — ArgoCD Application, ExternalSecret, Crossplane resources. Local copy at `./argocd-applications/`.

## Use uv — Always

Always use `uv` to run Python commands — installing dependencies, running tests, linting, and any script. Never call `python` or `.venv/bin/python` directly.

```bash
uv run <command>     # run a command in the project environment
uv sync              # install/sync dependencies
uv run pytest ...    # tests
uv run ruff check    # lint
```

## TDD — Always

Every implementation, bug fix, or adjustment must follow TDD:

1. **RED** — write a failing test that reproduces the bug or validates the expected behavior. Compile errors does not count as red state. 
2. **GREEN** — apply the minimal fix to make the test pass
3. Run the full suite:
   - `cd /Users/guilhermecastro/Projects/ckan-ingestor && uv run pytest tests/ -q`
   - `cd /Users/guilhermecastro/Projects/ckan-ingestor/ingestor_orchestrator/backend && uv run pytest tests/ -q`

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

## Repository Layer

Data access uses the **Repository pattern** with interface/implementation separation:

```
ingestor_orchestrator/repositories/
├── __init__.py                          # Public exports
├── job_repository.py                    # JobRepository ABC
├── instance_repository.py               # InstanceRepository ABC
├── resource_repository.py               # ResourceRepository ABC
├── sync_repository.py                   # SyncRepository ABC
├── dashboard_repository.py              # DashboardRepository ABC
├── sqlalchemy_job_repository.py         # SQLAlchemy impl
├── sqlalchemy_instance_repository.py    # SQLAlchemy impl
├── sqlalchemy_resource_repository.py    # SQLAlchemy impl
├── sqlalchemy_sync_repository.py        # SQLAlchemy impl
└── sqlalchemy_dashboard_repository.py   # SQLAlchemy impl
```

### Structure

- **Interface** (`*_repository.py`): Abstract Base Class defining the contract. No imports from `sqlalchemy`.
- **Implementation** (`sqlalchemy_*_repository.py`): SQLAlchemy queries, constructor takes `AsyncSession`.
- **Tests** (`tests/test_*_repository.py`): Test the implementation directly against SQLite, capturing SQL with `event.listen` to verify column selection and query patterns.

### API wiring

API endpoints inject repositories via FastAPI `Depends`:

```python
from fastapi import Depends
from ingestor_orchestrator.db import get_db
from ingestor_orchestrator.repositories import SqlAlchemyJobRepository

async def get_job_repository(db: AsyncSession = Depends(get_db)):
    return SqlAlchemyJobRepository(db)

@router.get("/")
async def list_jobs(repo: JobRepository = Depends(get_job_repository)):
    jobs, total = await repo.list_jobs(...)
```

DTO conversion stays in the API layer. Repositories return ORM objects.
