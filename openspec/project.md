# Project Context

## Purpose
This repository isolates the options backfill work that was split out of Macro_Ideas so it can evolve independently. The current scope covers the EODHD options helper, PostgreSQL persistence for backfill state, and the OpenSpec baseline for Milestones 0-1.

## Tech Stack
- Python 3.10+
- pandas for tabular shaping and date normalization
- DuckDB for local helper-level query caching
- asyncpg with PostgreSQL for durable backfill storage
- pytest for unit and integration validation
- OpenSpec for spec-driven planning and change tracking

## Project Conventions

### Code Style
- Prefer small typed functions and dataclasses over large unstructured dictionaries.
- Keep modules focused on one responsibility: provider access, models/config, storage, SQL.
- Preserve the existing standard-library-first style unless a new dependency is justified.
- Use concise docstrings where behavior or intent is not obvious from the code alone.

### Architecture Patterns
- `eodhd_options_helper.py` is the notebook-scale synchronous adapter with a DuckDB cache.
- `options_backfill/` is the durable backfill foundation for PostgreSQL-backed job and task state.
- SQL lives in `options_backfill/sql/` and is loaded by name from `queries.sql`.
- Provider orchestration beyond storage should be introduced in follow-up changes, not folded into storage primitives.

### Testing Strategy
- `python -m pytest -q` is the default validation command.
- Unit tests should run without external services.
- PostgreSQL integration tests may require local credentials and must be safe by default; they should only run against `test_database`.
- For repo hygiene changes, validate both the Python test suite and `openspec validate --strict` when OpenSpec files change.

### Git Workflow
- `main` is the default branch in this repository.
- Use OpenSpec proposals for new capabilities, breaking behavior changes, or architectural changes.
- Direct fixes are acceptable for bugs, documentation cleanup, test hygiene, and repo setup issues.

## Domain Context
- The primary provider today is the EODHD UnicornBay Options API.
- The first endpoint priority is `eod`; `contracts` is the second planned endpoint.
- Backfill state must be durable and resumable, with idempotent writes for `(contract, tradetime)` rows.

## Important Constraints
- Respect EODHD request quotas; one HTTP request maps to ten billed API call units in current assumptions.
- PostgreSQL is the durable system of record for backfill state; DuckDB is only a local helper cache.
- Integration tests must not accidentally target non-test databases.

## External Dependencies
- EODHD UnicornBay Options API for underlying symbols, contracts, and EOD records
- PostgreSQL for durable storage
- DuckDB for local cache persistence in the helper

