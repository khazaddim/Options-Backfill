<!-- OPENSPEC:START -->
# OpenSpec Instructions

These instructions are for AI assistants working in this project.

Always open `@/openspec/AGENTS.md` when the request:
- Mentions planning or proposals (words like proposal, spec, change, plan)
- Introduces new capabilities, breaking changes, architecture shifts, or big performance/security work
- Sounds ambiguous and you need the authoritative spec before coding

Use `@/openspec/AGENTS.md` to learn:
- How to create and apply change proposals
- Spec format and conventions
- Project structure and guidelines

Keep this managed block so 'openspec update' can refresh the instructions.

<!-- OPENSPEC:END -->

# Repository Instructions

## Purpose
This repository contains the extracted options backfill work that was split out of Macro_Ideas. The current implemented surface is the Massive helper, PostgreSQL storage layer, SQL schema/query files, and tests for Milestones 0-1.

## Working Rules
- Use `openspec/AGENTS.md` for OpenSpec workflow details.
- Read `openspec/project.md` and `openspec/specs/options-backfill/spec.md` before proposing spec-level changes.
- Treat bug fixes and repo hygiene fixes as direct changes unless they change behavior or architecture materially.
- Keep changes focused; avoid broad refactors unless a proposal has been approved.

## Validation
- Install the repo with `pip install -e .[dev]` in the project virtual environment.
- Run `python -m pytest -q` for the default test suite.
- PostgreSQL integration tests in `tests/test_options_backfill_storage.py` require `PG_TEST_CONFIG_PATH` pointing at a safe `test_database` config and otherwise should be skipped.

