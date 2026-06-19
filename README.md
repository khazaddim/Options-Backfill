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

## Massive Token Configuration

The helper resolves Massive credentials in this order:

1. Explicit `api_token` argument passed to helper functions
2. `api_token_file` (full JSON file path) plus `api_token_key` (exact top-level key name)

Example:

```powershell
python -c "import massive_options_helper as h; print(h.download_options_underlying_symbols(api_token_file=r'C:\Users\you\keys\massive.json', api_token_key='massive_api_token').head())"
```

Quick reference:

- `api_token_file` should be an absolute path to a JSON file anywhere on your machine (it does not need to be in this repo).
- `api_token_key` should match the exact top-level JSON key name containing your Massive token.

Example JSON file:

```json
{
	"massive_api_token": "YOUR_REAL_TOKEN"
}
```

Example helper call:

```python
import massive_options_helper as h

bars = h.download_options_time_series(
		contract="O:SPY250117P00450000",
		range_from="2025-01-01",
		range_to="2025-01-10",
		multiplier=15,
		timespan="minute",
		api_token_file=r"C:\Users\you\secrets\massive_keys.json",
		api_token_key="massive_api_token",
)
```

## Validation

Run the default test suite with:

```powershell
python -m pytest -q
```

Run optional live Massive integration tests with:

```powershell
python -m pytest -q tests/test_massive_options_helper_live.py --massive-live --massive-token-file "C:\full\path\to\your\keys.json" --massive-token-key "massive_api_token"
```

These live tests are opt-in and skipped by default so local/CI runs stay deterministic.

The PostgreSQL integration tests in `tests/test_options_backfill_storage.py` require `PG_TEST_CONFIG_PATH` to point to a JSON config for a database named `test_database`. If that config is not present, those tests are skipped.

## OpenSpec

OpenSpec is already initialized for this repository. The active baseline lives in `openspec/specs/options-backfill/spec.md`, and repo conventions for spec work live in `openspec/project.md`.


