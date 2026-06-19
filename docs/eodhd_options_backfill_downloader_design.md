# EODHD Options Backfill Downloader Design

## Purpose

This document specifies a Python design for asynchronously downloading historical US options data from the EODHD UnicornBay Options API into a dedicated PostgreSQL options database.

It builds on two existing pieces in this repository:

- `eodhd_options_helper.py`, which defines the EODHD endpoint shapes, response normalization, pagination behavior, and DuckDB read-through cache concept.
- `tests/test_eodhd_options_helper.py`, which proves the adapter can flatten EODHD-style responses and avoid repeat API calls through caching.

The goal of this next layer is a more durable downloader/backfill system that can retrieve full historical option EOD time series or option-chain slices across many symbols, expirations, strikes, and dates without overusing the API.

---

## Design Goals

### Functional goals

- Download EODHD options data asynchronously for one or more underlying symbols.
- Support full historical option EOD records and contract metadata.
- Store normalized options data in PostgreSQL using idempotent writes.
- Track backfill jobs, planned tasks, task status, failures, retries, and completion.
- Resume partially completed jobs without re-downloading already stored rows.
- Rate-limit outbound requests so large backfills respect vendor limits.
- Allow future strategy code to query historical option chains by symbol, trade date, expiration, strike, and option type.

### Non-functional goals

- Keep provider-specific EODHD logic isolated from storage and retry orchestration.
- Prefer PostgreSQL as the durable source of truth for backfill state.
- Keep a bounded async concurrency model with explicit throttling.
- Make all writes idempotent so retries and restarts are safe.
- Use small, testable components rather than one large downloader script.

---

## Relationship To Existing Files

### `eodhd_options_helper.py`

The current helper should remain useful for notebook-scale exploration and for shared parsing/query conventions. The downloader should either reuse or mirror these pieces:

- endpoint names: `underlying-symbols`, `contracts`, `eod`
- query parameter construction for filters such as `underlying_symbol`, `exp_date_*`, `tradetime_*`, `type`, and strike filters
- JSON flattening into tabular rows
- pagination behavior using `page[offset]` and `page[limit]`
- API token resolution from `api_token` or `EODHD_API_TOKEN`

The production backfill path should not rely only on the helper's DuckDB cache, because PostgreSQL will be the durable backfill database. DuckDB can still remain useful as a notebook-level local cache, but the async downloader should treat PostgreSQL as the primary cache, queue, and validation layer.

### `test_eodhd_options_helper.py`

The existing mocked tests are a good starting point for response-shape tests. The new downloader should add tests that simulate:

- paginated EODHD responses
- HTTP 429 rate-limit responses
- transient network errors
- duplicate rows
- partial backfills followed by resume
- task retry and final failure after retry exhaustion

---

## Recommended Module Layout

```text
Macro_Ideas/
  docs/
    backfill_module_design.md
    eodhd_options_backfill_downloader_design.md
  options_backfill/
    __init__.py
    config.py
    models.py
    eodhd_provider.py
    rate_limit.py
    storage.py
    planner.py
    downloader.py
    runner.py
    sql/
      schema.sql
      queries.sql
  tests/
    test_eodhd_options_helper.py
    test_eodhd_options_downloader.py
```

For a first version, this can start smaller with a single `eodhd_options_downloader.py` module. The split above is the direction to grow into once the workflow is proven.

---

## Core Public API

The main user-facing object should be a downloader class.

```python
class EODHDOptionsBackfillDownloader:
    async def backfill_eod_chain_history(self, config: OptionsBackfillConfig) -> OptionsBackfillResult:
        """Backfill historical EOD option chain records for a symbol/date/expiry/strike slice."""

    async def backfill_contract_metadata(self, config: OptionsBackfillConfig) -> OptionsBackfillResult:
        """Backfill option contract metadata for a symbol/expiration/strike slice."""

    async def backfill_underlying_symbols(self) -> int:
        """Refresh the supported underlying-symbol universe."""
```

The initial most useful method is `backfill_eod_chain_history`, because it supports the put-spread backtester directly. It should be able to fetch, for example:

- all SPY puts on a specific trade date and expiration
- all SPY puts in a strike band around 10% below spot
- a full two-year EOD time series for one specific option contract
- all option EOD rows for one symbol across a bounded `tradetime` date range

---

## Configuration Model

```python
from dataclasses import dataclass

@dataclass(slots=True)
class OptionsBackfillConfig:
    endpoint: str                         # "eod" or "contracts"
    underlying_symbol: str | None = None
    contract: str | None = None
    option_type: str | None = None         # "put" or "call"
    exp_date_eq: str | None = None
    exp_date_from: str | None = None
    exp_date_to: str | None = None
    tradetime_eq: str | None = None
    tradetime_from: str | None = None
    tradetime_to: str | None = None
    strike_eq: float | None = None
    strike_from: float | None = None
    strike_to: float | None = None
    fields: tuple[str, ...] | None = None
    sort: str | None = None
    page_limit: int = 1000
    max_pages_per_task: int | None = 25
    max_concurrent_requests: int = 8
    requests_per_minute: int = 90
    max_retries_per_task: int = 3
    request_timeout_seconds: int = 30
    retry_backoff_seconds: float = 1.0
    store_raw_json: bool = False
```

Notes:

- `requests_per_minute` should default well below the vendor maximum. EODHD lists 1,000 requests/minute and 100,000 API calls/day, with one request costing 10 API calls. A conservative default such as 90 requests/minute leaves safety room.
- `max_pages_per_task` prevents runaway broad queries from consuming too much quota accidentally.
- Broad universe-wide jobs should be split into tasks by symbol, date, expiration, or strike band rather than one huge query.

---

## Result Model

```python
from dataclasses import dataclass

@dataclass(slots=True)
class OptionsBackfillResult:
    job_id: int
    endpoint: str
    status: str
    requested_tasks: int
    completed_tasks: int
    failed_tasks: int
    inserted_rows: int
    updated_rows: int
    skipped_existing_rows: int
    api_requests_made: int
    api_call_units_estimated: int
    unresolved_tasks: list[int]
```

This result should be queryable from PostgreSQL as well as returned from Python.

---

## PostgreSQL Schema

Use a dedicated schema such as `options_data`.

### Underlying symbols

```sql
CREATE SCHEMA IF NOT EXISTS options_data;

CREATE TABLE IF NOT EXISTS options_data.underlying_symbols (
    underlying_symbol TEXT PRIMARY KEY,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

### Option contracts

```sql
CREATE TABLE IF NOT EXISTS options_data.contracts (
    contract TEXT PRIMARY KEY,
    underlying_symbol TEXT NOT NULL,
    exp_date DATE,
    expiration_type TEXT,
    option_type TEXT NOT NULL,
    strike DOUBLE PRECISION,
    exchange TEXT,
    currency TEXT,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw_json JSONB
);

CREATE INDEX IF NOT EXISTS idx_options_contracts_lookup
    ON options_data.contracts (underlying_symbol, exp_date, option_type, strike);
```

### EOD option records

```sql
CREATE TABLE IF NOT EXISTS options_data.option_eod (
    contract TEXT NOT NULL,
    tradetime DATE NOT NULL,
    underlying_symbol TEXT NOT NULL,
    exp_date DATE,
    expiration_type TEXT,
    option_type TEXT NOT NULL,
    strike DOUBLE PRECISION,
    exchange TEXT,
    currency TEXT,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    last DOUBLE PRECISION,
    last_size DOUBLE PRECISION,
    previous DOUBLE PRECISION,
    previous_date DATE,
    bid DOUBLE PRECISION,
    bid_date TIMESTAMPTZ,
    bid_size DOUBLE PRECISION,
    ask DOUBLE PRECISION,
    ask_date TIMESTAMPTZ,
    ask_size DOUBLE PRECISION,
    midpoint DOUBLE PRECISION,
    moneyness DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    open_interest DOUBLE PRECISION,
    volatility DOUBLE PRECISION,
    theoretical DOUBLE PRECISION,
    delta DOUBLE PRECISION,
    gamma DOUBLE PRECISION,
    theta DOUBLE PRECISION,
    vega DOUBLE PRECISION,
    rho DOUBLE PRECISION,
    dte INTEGER,
    vol_oi_ratio DOUBLE PRECISION,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw_json JSONB,
    PRIMARY KEY (contract, tradetime)
);

CREATE INDEX IF NOT EXISTS idx_option_eod_chain_lookup
    ON options_data.option_eod (underlying_symbol, tradetime, exp_date, option_type, strike);

CREATE INDEX IF NOT EXISTS idx_option_eod_contract_time
    ON options_data.option_eod (contract, tradetime);
```

### Backfill jobs

```sql
CREATE TABLE IF NOT EXISTS options_data.backfill_jobs (
    job_id BIGSERIAL PRIMARY KEY,
    endpoint TEXT NOT NULL,
    status TEXT NOT NULL,
    underlying_symbol TEXT,
    contract TEXT,
    option_type TEXT,
    exp_date_eq DATE,
    exp_date_from DATE,
    exp_date_to DATE,
    tradetime_eq DATE,
    tradetime_from DATE,
    tradetime_to DATE,
    strike_eq DOUBLE PRECISION,
    strike_from DOUBLE PRECISION,
    strike_to DOUBLE PRECISION,
    page_limit INTEGER NOT NULL,
    max_pages_per_task INTEGER,
    max_retries_per_task INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

### Backfill tasks

```sql
CREATE TABLE IF NOT EXISTS options_data.backfill_tasks (
    task_id BIGSERIAL PRIMARY KEY,
    job_id BIGINT NOT NULL REFERENCES options_data.backfill_jobs(job_id),
    endpoint TEXT NOT NULL,
    query_params JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    priority INTEGER NOT NULL DEFAULT 100,
    retry_count INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 3,
    page_offset INTEGER NOT NULL DEFAULT 0,
    page_limit INTEGER NOT NULL DEFAULT 1000,
    max_pages INTEGER,
    rows_received INTEGER NOT NULL DEFAULT 0,
    rows_inserted INTEGER NOT NULL DEFAULT 0,
    api_requests_made INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    claimed_by TEXT,
    claimed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_options_backfill_tasks_claim
    ON options_data.backfill_tasks (status, priority, created_at);

CREATE INDEX IF NOT EXISTS idx_options_backfill_tasks_job_status
    ON options_data.backfill_tasks (job_id, status);
```

### Optional request log

```sql
CREATE TABLE IF NOT EXISTS options_data.api_request_log (
    request_id BIGSERIAL PRIMARY KEY,
    job_id BIGINT REFERENCES options_data.backfill_jobs(job_id),
    task_id BIGINT REFERENCES options_data.backfill_tasks(task_id),
    endpoint TEXT NOT NULL,
    url_without_token TEXT NOT NULL,
    status_code INTEGER,
    row_count INTEGER,
    elapsed_ms INTEGER,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

This table is useful for quota visibility, debugging, and estimating how expensive different backfill patterns are.

---

## Downloader Class Responsibilities

```python
class EODHDOptionsBackfillDownloader:
    def __init__(self, pool, api_token: str, *, base_url: str = OPTIONS_API_BASE_URL):
        self.pool = pool
        self.api_token = api_token
        self.base_url = base_url

    async def backfill_eod_chain_history(self, config: OptionsBackfillConfig) -> OptionsBackfillResult:
        ...

    async def backfill_contract_metadata(self, config: OptionsBackfillConfig) -> OptionsBackfillResult:
        ...

    async def run_job(self, job_id: int) -> OptionsBackfillResult:
        ...
```

The downloader should own orchestration, not low-level SQL details. SQL should live in a storage layer.

### Downloader responsibilities

- validate configuration
- create a backfill job row
- plan tasks from broad query ranges
- start bounded async workers
- claim tasks from PostgreSQL
- call the provider adapter
- pass normalized rows to storage insert functions
- update task and job state
- summarize final result

### Downloader non-responsibilities

- raw SQL string ownership
- detailed EODHD response parsing
- strategy-specific option selection logic
- notebook visualization
- direct manipulation of the put-spread backtester

---

## Provider Adapter Responsibilities

The provider adapter should be the async version of the endpoint logic in `eodhd_options_helper.py`.

```python
class EODHDOptionsProvider:
    async def fetch_page(
        self,
        endpoint: str,
        params: dict[str, object],
        *,
        offset: int,
        limit: int,
    ) -> EODHDPage:
        ...

    async def fetch_all_pages(
        self,
        endpoint: str,
        params: dict[str, object],
        *,
        max_pages: int | None,
    ) -> list[dict[str, object]]:
        ...
```

### Suggested page model

```python
@dataclass(slots=True)
class EODHDPage:
    endpoint: str
    offset: int
    limit: int
    total: int | None
    next_url: str | None
    rows: list[dict[str, object]]
    raw_json: dict[str, object] | None = None
```

The adapter should:

- build URLs
- use `aiohttp.ClientSession`
- attach the API token
- decode JSON
- flatten `data[*].attributes`
- preserve response IDs where useful
- return normalized row dictionaries
- raise typed exceptions for retryable versus fatal errors

---

## Async Concurrency And Rate Limiting

The API limits make async useful but only with guardrails.

EODHD states:

- 100,000 API calls per 24 hours
- 1,000 API requests per minute
- 1 API request = 10 API calls

### Recommended v1 controls

- `asyncio.Semaphore` for max concurrent HTTP requests.
- Token-bucket or leaky-bucket limiter for requests per minute.
- `max_pages_per_task` to prevent accidental huge pulls.
- Retry with exponential backoff and jitter for HTTP 429, 500, 502, 503, 504, and network timeouts.
- Request logging to PostgreSQL.

### Rate limiter sketch

```python
class AsyncRateLimiter:
    def __init__(self, requests_per_minute: int):
        self.interval = 60.0 / requests_per_minute
        self._lock = asyncio.Lock()
        self._next_allowed_at = 0.0

    async def acquire(self) -> None:
        async with self._lock:
            now = asyncio.get_running_loop().time()
            wait_seconds = max(0.0, self._next_allowed_at - now)
            if wait_seconds:
                await asyncio.sleep(wait_seconds)
            self._next_allowed_at = max(now, self._next_allowed_at) + self.interval
```

This simple limiter is conservative and process-local. It can be replaced later with a PostgreSQL-backed limiter if multiple Python processes will run at once.

---

## Task Planning

The planner should avoid creating huge unbounded API calls. The safest v1 approach is to split broad requests into deterministic slices.

### EOD chain history planning

For a strategy like weekly SPY put spreads, useful task slices are:

- one underlying symbol
- one `tradetime` day or week
- one expiration date or expiration range
- one option type
- one strike band

Example task query:

```python
{
    "filter[underlying_symbol]": "SPY",
    "filter[type]": "put",
    "filter[tradetime_eq]": "2025-01-03",
    "filter[exp_date_eq]": "2025-01-10",
    "filter[strike_from]": 520.0,
    "filter[strike_to]": 590.0,
    "sort": "strike",
    "page[limit]": 1000,
}
```

For multi-stock research, split by symbol first and then by date. This makes failures and resumes easy to reason about.

### Contract metadata planning

Contract metadata can be planned by:

- symbol
- expiration month or expiration range
- option type
- optional strike band

The contracts endpoint can be refreshed less often than EOD records because metadata changes more slowly.

---

## Backfill Flow

```text
1. User creates OptionsBackfillConfig.
2. Downloader validates the config.
3. Downloader creates options_data.backfill_jobs row.
4. Planner expands the config into options_data.backfill_tasks rows.
5. Downloader starts N async workers.
6. Each worker claims one pending task with FOR UPDATE SKIP LOCKED.
7. Worker calls EODHDOptionsProvider with rate limiter and retry policy.
8. Provider returns normalized rows.
9. Storage inserts rows into option_eod, contracts, or underlying_symbols.
10. Worker updates task rows_received, rows_inserted, api_requests_made, and status.
11. Failed retryable tasks return to pending until retry_count reaches max_retries.
12. Downloader summarizes job completion from PostgreSQL.
13. Final result is returned to notebook or script code.
```

---

## Safe Task Claiming

Use the same durable queue pattern from the broader backfill design.

```sql
WITH next_task AS (
    SELECT task_id
    FROM options_data.backfill_tasks
    WHERE status = 'pending'
    ORDER BY priority, created_at
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
UPDATE options_data.backfill_tasks t
SET
    status = 'running',
    claimed_by = $1,
    claimed_at = NOW(),
    updated_at = NOW()
FROM next_task
WHERE t.task_id = next_task.task_id
RETURNING t.*;
```

This allows multiple async workers to run without claiming the same task.

---

## Idempotent Inserts

### EOD option rows

```sql
INSERT INTO options_data.option_eod (...)
VALUES (...)
ON CONFLICT (contract, tradetime) DO UPDATE SET
    bid = EXCLUDED.bid,
    ask = EXCLUDED.ask,
    midpoint = EXCLUDED.midpoint,
    volatility = EXCLUDED.volatility,
    delta = EXCLUDED.delta,
    gamma = EXCLUDED.gamma,
    theta = EXCLUDED.theta,
    vega = EXCLUDED.vega,
    rho = EXCLUDED.rho,
    fetched_at = NOW(),
    raw_json = COALESCE(EXCLUDED.raw_json, options_data.option_eod.raw_json);
```

For research, updating on conflict is useful because vendor records can be revised or enriched. If preserving first-seen values matters, switch to `DO NOTHING` and store revisions separately.

### Contract rows

```sql
INSERT INTO options_data.contracts (...)
VALUES (...)
ON CONFLICT (contract) DO UPDATE SET
    underlying_symbol = EXCLUDED.underlying_symbol,
    exp_date = EXCLUDED.exp_date,
    option_type = EXCLUDED.option_type,
    strike = EXCLUDED.strike,
    last_seen_at = NOW(),
    raw_json = COALESCE(EXCLUDED.raw_json, options_data.contracts.raw_json);
```

---

## PostgreSQL As Cache

Before creating a task or calling the API, the downloader should ask PostgreSQL what is already stored.

Examples:

- If `option_eod` already has all rows for a known contract and `tradetime` window, skip the task.
- If a task was completed previously, do not enqueue it again unless `force_refresh=True`.
- If a broad chain query has already been fetched for the same symbol/date/expiration/strike band, reuse the stored rows.

The first v1 can be simpler: rely on task state and idempotent writes. A later version can add preflight row-existence checks to avoid even starting redundant tasks.

---

## Error Handling

### Retryable errors

- HTTP 429
- HTTP 500, 502, 503, 504
- request timeout
- connection reset
- malformed temporary response if recoverable

### Fatal errors

- missing API token
- invalid config
- HTTP 401 or 403
- unsupported endpoint
- invalid date or strike filter
- `max_pages_per_task` exceeded, unless explicitly configured as retryable

### Backoff policy

Use exponential backoff with jitter:

```python
delay = base_delay * (2 ** retry_count) + random.uniform(0, base_delay)
```

For HTTP 429, respect `Retry-After` if the provider sends it.

---

## Logging And Observability

Log both Python events and database request rows.

Minimum useful events:

- job created
- tasks planned
- task claimed
- API request started
- API request completed with status, rows, elapsed time
- task completed
- task failed and scheduled for retry
- task exhausted retries
- job complete or partial

Minimum useful counters:

- API requests made
- estimated API call units consumed
- rows received
- rows inserted or updated
- tasks pending/running/completed/failed

---

## Strategy Integration Path

The put-spread backtester needs a historical premium credit. Once `option_eod` is populated, a strategy helper can select historical legs like this:

```sql
SELECT *
FROM options_data.option_eod
WHERE underlying_symbol = $1
  AND tradetime = $2
  AND exp_date = $3
  AND option_type = 'put'
  AND strike BETWEEN $4 AND $5
ORDER BY ABS(strike - $6)
LIMIT 1;
```

For a 10% OTM short put and $5-wide spread:

1. Get SPY close on entry date.
2. Target short strike = `close * 0.90`.
3. Query nearest available put strike to target.
4. Target long strike = `short_strike - 5`.
5. Query nearest available put strike to target long strike.
6. Entry credit = `short_leg_midpoint - long_leg_midpoint`.
7. Feed that credit into `PutSpreadBacktester` through `premium_credit` or a `premium_model` callback.

---

## Testing Strategy

### Unit tests

- query parameter construction
- URL building without leaking tokens in logs
- response flattening
- row normalization and type conversion
- idempotent insert behavior
- rate limiter timing behavior with a fake clock if practical

### Integration-style tests with mocked HTTP

- one-page response
- multi-page response
- `max_pages_per_task` stops runaway pagination
- 429 then success
- 500 then success
- permanent 403 fails fast
- duplicate rows remain idempotent
- resume a partially completed job

### PostgreSQL tests

Use a disposable test database if available, or isolate these behind an integration marker.

For `options_backfill` module testing in this repository, real PostgreSQL integration tests are the default path (using the dedicated local `test_database`) so schema, task lifecycle, and idempotent upsert behavior are validated against an actual database.

Success criteria:

- job row is created
- tasks are enqueued
- concurrent workers claim different tasks
- option rows are inserted idempotently
- failed tasks retry and eventually complete or fail predictably

---

## OpenSpec Sync Policy

This file is the roadmap source used to guide OpenSpec change requests for this project.

Sync rules:

- Each OpenSpec change request must reference one or more milestone numbers from this file.
- Milestone deliverables and definition-of-done text in OpenSpec should stay semantically aligned with this file.
- If roadmap intent changes, update this file first, then update the corresponding OpenSpec proposal, design, tasks, and spec delta.
- If implementation detail changes within an approved milestone scope, update OpenSpec artifacts first, then back-propagate any roadmap wording changes needed here.
- Before approving a milestone change request, run a quick drift check between this file and the OpenSpec change directory.

Current mapping:

- Milestones 0-1 are tracked by OpenSpec change `add-eodhd-backfill-m0-m1`.

---

## Implementation Milestones

### Milestone 0 - Scope Lock And Interfaces

Goal: freeze v1 scope so implementation can proceed in small PRs.

Deliverables:

- Finalize v1 endpoint priority (`eod` first, `contracts` second).
- Freeze v1 module layout (single-file downloader or split package).
- Freeze the `OptionsBackfillConfig` and `OptionsBackfillResult` shapes.

Definition of done:

- Spec reflects final v1 choices with no unresolved blocking design decisions.

Expected file changes (illustrative, not strict):

- Must add: none
- May add: none
- Should not add yet: `options_backfill/` implementation modules

### Milestone 1 - Database Foundation

Goal: make PostgreSQL the durable state layer before network orchestration.

Deliverables:

- Create `options_data` schema and core tables.
- Add indexes for chain lookup and task claiming.
- Implement async storage helpers (`create_job`, `enqueue_tasks`, `insert_eod_rows`, task state updates).

Definition of done:

- Idempotent `option_eod` upsert works.
- A job and at least one task can be created, claimed, and marked complete in the DB.

Expected file changes (illustrative, not strict):

- Must add:
    - `options_backfill/sql/schema.sql`
    - `options_backfill/sql/queries.sql`
    - storage implementation (`options_backfill/storage.py` or `eodhd_options_downloader.py`)
- May add:
    - `options_backfill/models.py`
    - `options_backfill/config.py`
- Should not add yet:
    - full async provider and worker orchestration modules

### Milestone 2 - Async Provider (No Retries Yet)

Goal: fetch EODHD pages asynchronously with normalized output.

Deliverables:

- `aiohttp` provider client.
- Endpoint URL/query builder reuse from `eodhd_options_helper.py`.
- Response flattening (`data[*].attributes` -> row dicts).
- Pagination support via `page[offset]` and `page[limit]`.

Definition of done:

- One task can download multi-page EOD data and return normalized rows.
- Unit tests cover one-page and multi-page behavior.

Expected file changes (illustrative, not strict):

- Must add:
    - provider implementation (`options_backfill/eodhd_provider.py` or `eodhd_options_downloader.py`)
    - provider-focused tests (`tests/test_eodhd_options_downloader.py`)
- May add:
    - shared normalization helper module if split from provider
- Should not add yet:
    - retry engine and rate-limiter module unless required to satisfy tests

### Milestone 3 - Safety Controls (Rate Limit + Backoff)

Goal: prevent quota spikes and support recoverable failures.

Deliverables:

- Process-local async rate limiter.
- Retry policy for 429 and transient 5xx/network errors.
- Exponential backoff with jitter.
- `max_pages_per_task` guardrail.

Definition of done:

- Mocked tests confirm 429 retry and max-page stop behavior.
- Request flow stays within configured request-rate ceiling.

Expected file changes (illustrative, not strict):

- Must add:
    - safety controls in provider/downloader path (rate limit, retry, max-pages)
- May add:
    - `options_backfill/rate_limit.py`
    - `options_backfill/errors.py` for typed retryable/fatal exceptions
- Should not add yet:
    - full task-queue worker orchestration if not needed by this milestone

### Milestone 4 - Worker Loop And Task Orchestration

Goal: run end-to-end backfill for one bounded query slice.

Deliverables:

- `EODHDOptionsBackfillDownloader` class with worker pool.
- `FOR UPDATE SKIP LOCKED` task claiming.
- Task lifecycle updates: `pending` -> `running` -> `completed|failed`.
- Job summary updates.

Definition of done:

- Concurrent workers process different tasks safely.
- A job can run from created -> complete with rows persisted.

Expected file changes (illustrative, not strict):

- Must add:
    - downloader orchestration (`options_backfill/downloader.py` or `eodhd_options_downloader.py`)
    - task-claim/worker-loop logic
- May add:
    - `options_backfill/runner.py`
    - `options_backfill/planner.py`
- Should not add yet:
    - strategy-specific leg-selection helpers

### Milestone 5 - Resume And Partial Recovery

Goal: make the downloader resumable and reliable across interruptions.

Deliverables:

- Resume support for unfinished jobs/tasks.
- Retry exhaustion handling and final partial status.
- Optional task deduplication preflight for identical query slices.

Definition of done:

- Interrupting and rerunning the same job continues instead of duplicating work.
- Failed tasks become retryable or terminal based on policy.

Expected file changes (illustrative, not strict):

- Must add:
    - resume/recovery logic in downloader and storage layers
    - tests for interruption and rerun behavior
- May add:
    - task dedupe helper module
- Should not add yet:
    - notebook integration code

### Milestone 6 - Strategy Query Helpers

Goal: expose data access helpers needed by options strategy backtests.

Deliverables:

- Helper for historical chain slice query by symbol/tradetime/exp/type/strike band.
- Helper for nearest-strike leg selection.
- Helper to compute midpoint spread credit from stored legs.

Definition of done:

- One query path can reproduce short/long put leg selection for a target date.

Expected file changes (illustrative, not strict):

- Must add:
    - strategy query helpers (new module or downloader subcomponent)
    - tests for nearest-strike and spread-credit helper behavior
- May add:
    - `options_backfill/planner.py` if query planning and selection are split
- Should not add yet:
    - broad notebook UI/reporting layers

### Milestone 7 - Notebook Integration Demo

Goal: prove workflow utility with existing notebook/backtester code.

Deliverables:

- Notebook section showing one historical SPY chain retrieval from PostgreSQL.
- Leg selection and credit calculation demonstration.
- Small backtest run with historical midpoint credits.

Definition of done:

- Notebook cell sequence runs end-to-end and produces a non-synthetic premium-based sample.

Expected file changes (illustrative, not strict):

- Must add:
    - notebook demo cells using PostgreSQL-backed option chain pulls
- May add:
    - small notebook helper wrappers for display and diagnostics
- Should not add yet:
    - major framework-level refactor unless it resolves a blocker

Guidance: These file expectations are intentionally illustrative, not hard constraints. Milestone success is determined by deliverables and definition-of-done behavior, not by exact filenames.

### Suggested PR Slicing

- PR 1: Milestones 0-1
- PR 2: Milestone 2
- PR 3: Milestone 3
- PR 4: Milestone 4
- PR 5: Milestones 5-6
- PR 6: Milestone 7

This slicing keeps each change reviewable and testable without blocking future milestones.

---

## Minimal First Usable Version

The smallest useful version is:

1. PostgreSQL schema for `option_eod`, `contracts`, `backfill_jobs`, and `backfill_tasks`.
2. Async provider for the `options/eod` endpoint.
3. Rate-limited paginated fetch.
4. Idempotent insert into `option_eod`.
5. Downloader method for one symbol, one date range, one expiration, one option type, and one strike band.
6. Query helper that returns a chain slice for a specific trade date and expiration.

That is enough to start replacing fixed-premium put-spread demos with real historical midpoint credits.

---

## Open Questions

- Should raw JSON be stored for every row or only for debug runs?
- Should EOD rows update existing values on conflict or preserve first-seen values?
- Should broad chain backfills be planned by daily `tradetime_eq` tasks or weekly windows?
- Should the production cache be PostgreSQL only, or should DuckDB remain as a local notebook cache as well?
- How conservative should the default request rate be compared with the vendor limit?

---

## Summary

The recommended design is a PostgreSQL-backed async downloader with EODHD-specific provider logic isolated behind an adapter. The current `eodhd_options_helper.py` gives the endpoint and normalization foundation. The next layer should add durable job/task state, async pagination, rate limiting, retries, and idempotent PostgreSQL writes.

This design keeps the path clear from raw historical option-chain downloads to strategy use: once the EOD rows are in PostgreSQL, the put-spread backtester can select historical short and long put legs directly from stored chain data and use actual midpoint credits instead of fixed or model-implied premiums.