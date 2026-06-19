# Options-Backfill

Options-Backfill is the extracted options backfill portion of Macro_Ideas. It currently includes:

- `massive_options_helper.py` for Massive endpoint access and DuckDB-backed local caching, including intraday options bars (for example, 15-minute series).
- `options_backfill/` for PostgreSQL schema, storage primitives, and typed config/result models.
- `openspec/` for specification-driven planning and change management.

## Setup

Create or activate a virtual environment, then install the project and dev tools:

```powershell
pip install -e .[dev]
```

Core runtime dependencies:

- `pandas`
- `duckdb`
- `asyncpg`

## Validation

Run the default test suite with:

```powershell
python -m pytest -q
```

The PostgreSQL integration tests in `tests/test_options_backfill_storage.py` require `PG_TEST_CONFIG_PATH` to point to a JSON config for a database named `test_database`. If that config is not present, those tests are skipped.

## OpenSpec

OpenSpec is already initialized for this repository. The active baseline lives in `openspec/specs/options-backfill/spec.md`, and repo conventions for spec work live in `openspec/project.md`.


