from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any, Protocol

from .config import OptionsBackfillConfig
from .models import OptionsBackfillResult


class _AsyncConnection(Protocol):
    async def execute(self, query: str, *args: Any) -> Any:
        ...

    async def fetchrow(self, query: str, *args: Any) -> Any:
        ...

    async def fetchval(self, query: str, *args: Any) -> Any:
        ...


class _AcquireContextManager(Protocol):
    async def __aenter__(self) -> _AsyncConnection:
        ...

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        ...


class _AsyncPool(Protocol):
    '''
    _AsyncPool protocol in storage.py is just a structural type declaration saying: 
    "I expect any object with an acquire() method that works like this." 
    It's not meant to be implemented—it's a duck-typing contract that asyncpg.
    Pool already fulfills.
    '''
    def acquire(self) -> _AcquireContextManager:
        ...


def _load_named_sql_queries(file_path: Path) -> dict[str, str]:
    """Load SQL snippets marked with '-- name: <query_name>' into a dictionary."""
    query_map: dict[str, list[str]] = {}
    current_name: str | None = None

    for raw_line in file_path.read_text(encoding="utf-8").splitlines():
        if raw_line.startswith("-- name:"):
            current_name = raw_line.split(":", 1)[1].strip()
            query_map[current_name] = []
            continue
        if current_name is not None:
            query_map[current_name].append(raw_line)

    return {name: "\n".join(lines).strip() for name, lines in query_map.items()}


def _coerce_date(value: object) -> date | None:
    """Convert ISO date strings into date objects for asyncpg DATE bindings."""
    if value is None:
        return None
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        return date.fromisoformat(value)
    raise TypeError(f"Expected an ISO date string or date object, got: {type(value).__name__}")


class OptionsBackfillStorage:
    """PostgreSQL storage helpers for durable options backfill job state."""

    def __init__(self, pool: _AsyncPool, *, sql_dir: Path | None = None) -> None:
        self.pool = pool
        self.sql_dir = sql_dir or Path(__file__).with_name("sql")
        self.schema_sql_path = self.sql_dir / "schema.sql"
        self.queries_sql_path = self.sql_dir / "queries.sql"
        self.queries = _load_named_sql_queries(self.queries_sql_path)

    async def initialize_schema(self) -> None:
        schema_sql = self.schema_sql_path.read_text(encoding="utf-8")
        async with self.pool.acquire() as connection:
            await connection.execute(schema_sql)

    async def create_job(self, config: OptionsBackfillConfig) -> int:
        async with self.pool.acquire() as connection:
            job_id = await connection.fetchval(
                self.queries["create_job"],
                config.endpoint,
                config.underlying_symbol,
                config.contract,
                config.option_type,
                _coerce_date(config.exp_date_eq),
                _coerce_date(config.exp_date_from),
                _coerce_date(config.exp_date_to),
                _coerce_date(config.tradetime_eq),
                _coerce_date(config.tradetime_from),
                _coerce_date(config.tradetime_to),
                config.strike_eq,
                config.strike_from,
                config.strike_to,
                config.page_limit,
                config.max_pages_per_task,
                config.max_retries_per_task,
            )
        return int(job_id)

    async def enqueue_tasks(
        self,
        *,
        job_id: int,
        endpoint: str,
        task_query_params: list[dict[str, object]],
        max_retries: int,
        page_limit: int,
        max_pages: int | None,
        start_priority: int = 100,
    ) -> int:
        inserted = 0
        async with self.pool.acquire() as connection:
            for index, params in enumerate(task_query_params):
                priority = start_priority + index
                await connection.fetchval(
                    self.queries["insert_task"],
                    job_id,
                    endpoint,
                    json.dumps(params, sort_keys=True),
                    priority,
                    max_retries,
                    page_limit,
                    max_pages,
                )
                inserted += 1
        return inserted

    async def claim_next_task(self, worker_id: str) -> dict[str, Any] | None:
        async with self.pool.acquire() as connection:
            row = await connection.fetchrow(self.queries["claim_next_task"], worker_id)
        return dict(row) if row is not None else None

    async def mark_task_completed(
        self,
        *,
        task_id: int,
        rows_received: int,
        rows_inserted: int,
        api_requests_made: int,
    ) -> None:
        async with self.pool.acquire() as connection:
            await connection.execute(
                self.queries["mark_task_completed"],
                task_id,
                rows_received,
                rows_inserted,
                api_requests_made,
            )

    async def mark_task_failed(self, *, task_id: int, error_message: str, retryable: bool) -> None:
        async with self.pool.acquire() as connection:
            await connection.execute(self.queries["mark_task_failed"], task_id, error_message, retryable)

    async def set_job_status(self, job_id: int, status: str) -> None:
        async with self.pool.acquire() as connection:
            await connection.execute(self.queries["set_job_status"], job_id, status)

    async def upsert_option_eod_rows(
        self,
        rows: list[dict[str, object]],
        *,
        store_raw_json: bool,
    ) -> int:
        if not rows:
            return 0

        async with self.pool.acquire() as connection:
            for row in rows:
                raw_json_value = json.dumps(row, default=str) if store_raw_json else None
                await connection.execute(
                    self.queries["upsert_option_eod_row"],
                    row.get("contract"),
                    _coerce_date(row.get("tradetime")),
                    row.get("underlying_symbol"),
                    _coerce_date(row.get("exp_date")),
                    row.get("option_type") or row.get("type"),
                    row.get("strike"),
                    row.get("bid"),
                    row.get("ask"),
                    row.get("midpoint"),
                    row.get("volatility"),
                    row.get("delta"),
                    row.get("gamma"),
                    row.get("theta"),
                    row.get("vega"),
                    row.get("rho"),
                    raw_json_value,
                )

        return len(rows)

    async def summarize_job(self, job_id: int) -> OptionsBackfillResult:
        async with self.pool.acquire() as connection:
            row = await connection.fetchrow(self.queries["summarize_job"], job_id)

        if row is None:
            raise ValueError(f"Backfill job {job_id} was not found.")

        unresolved = list(row["unresolved_tasks"]) if row["unresolved_tasks"] else []
        requests_made = int(row["api_requests_made"])

        return OptionsBackfillResult(
            job_id=int(row["job_id"]),
            endpoint=str(row["endpoint"]),
            status=str(row["status"]),
            requested_tasks=int(row["requested_tasks"]),
            completed_tasks=int(row["completed_tasks"]),
            failed_tasks=int(row["failed_tasks"]),
            inserted_rows=int(row["inserted_rows"]),
            updated_rows=0,
            skipped_existing_rows=0,
            api_requests_made=requests_made,
            api_call_units_estimated=requests_made * 10,
            unresolved_tasks=[int(task_id) for task_id in unresolved],
        )
