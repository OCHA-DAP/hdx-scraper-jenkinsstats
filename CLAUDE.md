# CLAUDE.md

## Project Overview

**hdx-scraper-jenkinsstats** reads the HDX datastore from the `jenkins-builds` dataset (produced by hdx-scraper-jenkinsbuilds), computes monthly per-pipeline summary statistics, writes them to the `jenkins-stats` HDX dataset, and uploads the resulting CSV to Google Drive.

## Key Files

- `src/hdx/scraper/jenkinsstats/__main__.py` — entry point; loads config, runs retriever
- `src/hdx/scraper/jenkinsstats/jenkins_stats_retriever.py` — core `JenkinsStatsRetriever` class (reads builds dump, computes stats with pandas, writes to HDX datastore, uploads CSV)
- `src/hdx/scraper/jenkinsstats/config/project_configuration.yaml` — HDX dataset names, Google Drive folder ID

## Running

```bash
uv run python -m hdx.scraper.jenkinsstats
```

Requires:
- `HDX_KEY` env var — HDX API key
- `GOOGLE_SERVICE_ACCOUNT` env var — JSON service account credentials for Google Drive upload

## Testing

```bash
uv run pytest
```

Tests in `tests/test_jenkins_stats_retriever.py` cover `JenkinsStatsRetriever.process()` using mocked HDX API responses and pandas DataFrames.

## Code Style

- Formatted with `ruff` via pre-commit hooks (`uv run ruff format --check` to verify)
- Python ≥ 3.13
- Dependencies managed with `uv` (`uv sync` to install, `uv lock --upgrade` to update lockfile)

## Collaboration Style

- Be objective, not agreeable. Act as a partner, not a sycophant. Push back when you disagree, flag tradeoffs honestly, and don't sugarcoat problems.
- Keep explanations brief and to the point.
- Don't rely on recalled knowledge for facts that could be stale (API behaviour, library versions, external systems). Search or read the actual source first. If you lack verified information, say so rather than speculate.

## Scope of Changes

When fixing a bug or addressing PR feedback, change only what is necessary to resolve the specific issue. Do not refactor surrounding code, rename variables, adjust formatting, or make improvements in the same commit unless they are directly required by the fix. Unrelated changes obscure the intent of the fix and complicate review and blame.
