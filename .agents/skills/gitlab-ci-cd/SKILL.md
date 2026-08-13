---
name: gitlab-ci-cd
description: Monitor and fix GitLab CI/CD pipelines using glab CLI. Use this when the user wants to check pipeline status, debug failed jobs, retry pipelines, or manage merge requests on the project's GitLab repository.
---

# GitLab CI/CD — Monitor and Fix

You are a CI/CD assistant focused on this project's GitLab repository.

## Project Context

- Pipeline defined in `.gitlab-ci.yml` with three stages: `test`, `build`, `deploy`
- **test**: runs `uv sync && uv run ruff check && uv run pytest`
- **build**: builds Docker images for ingestor and orchestrator (`buildx`)
- **deploy**: deploys ingestor (OpenTofu) and orchestrator (Helm chart)
- Primary branch: `main`
- Linter: `ruff` (configured via `pyproject.toml` dev dependencies, default rules)

## Pre-Commit Lint Rule (MANDATORY)

**Always run lint before committing any Python changes.** The CI `test` stage runs `uv run ruff check` and will fail if lint errors exist.

Before creating a commit or suggesting one, run:

```bash
uv run ruff check
```

If there are errors:
- Fix them with `uv run ruff check --fix` for auto-fixable issues
- Manually fix any remaining issues
- Re-run `uv run ruff check` to confirm zero errors before proceeding

This prevents CI pipeline failures on the `test` stage.

## Instructions

### ALWAYS pass `--repo` to every `glab` command

This checkout is shared between two remotes (`pedalin/ckan-ingestor` and
`noctcloud/argocd-applications`), so `glab` may prompt interactively for which
base repository to use — which hangs non-interactive shells. Always disambiguate
with `--repo` on **every** `glab` invocation, e.g.:

```bash
glab ci status --repo pedalin/ckan-ingestor
glab ci view <pipeline-id> --repo pedalin/ckan-ingestor
glab ci trace <job-id> --repo pedalin/ckan-ingestor
glab ci retry <job-id> --repo pedalin/ckan-ingestor
glab mr list --repo pedalin/ckan-ingestor
glab mr view <id> --repo pedalin/ckan-ingestor
```

When the work concerns the GitOps repo instead, use
`--repo noctcloud/argocd-applications`.

1. Use `glab` to check current pipeline and job status:
   - `glab ci status --repo pedalin/ckan-ingestor` — list recent pipelines
   - `glab ci view <pipeline-id> --repo pedalin/ckan-ingestor` — pipeline details
   - `glab ci trace <job-id> --repo pedalin/ckan-ingestor` — job logs

2. If there are failures:
   - Analyze the job log with `glab ci trace --repo pedalin/ckan-ingestor`
   - Identify the root cause (lint error, test failure, build issue, deploy error)
   - Fix the relevant files in the project
   - Explain what caused the failure and what was fixed

3. If you need to re-run:
   - `glab ci retry <job-id> --repo pedalin/ckan-ingestor` — retry a specific job

4. For merge requests:
   - `glab mr list --repo pedalin/ckan-ingestor` — list open MRs
   - `glab mr view <id> --repo pedalin/ckan-ingestor` — MR details

## Available Tools

- `glab` — GitLab CLI
- Filesystem access to the project for editing Python, Docker, Helm, and OpenTofu files
- `uv run ruff check` / `uv run ruff check --fix` — Python linting
- `uv run pytest` — Python tests
