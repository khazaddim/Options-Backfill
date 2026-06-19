from __future__ import annotations

"""Unit tests for options_backfill storage helpers.

This module uses fake in-memory asyncpg-like objects and does not require a
real PostgreSQL database. It verifies storage wiring and control flow, such as
named SQL loading, job/task lifecycle method calls, and upsert call paths.

Compared to tests/test_options_backfill_storage.py, these tests focus on
behavioral plumbing and return-value shaping, not real SQL execution semantics.
"""

import asyncio
from pathlib import Path
from typing import Any

from options_backfill.config import OptionsBackfillConfig
from options_backfill.storage import OptionsBackfillStorage, _load_named_sql_queries


class _FakeConnection:
    """Minimal asyncpg-like connection test double that records every query call."""

    def __init__(self) -> None:
        self.fetchval_results: list[Any] = []
        self.fetchrow_results: list[Any] = []
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    async def execute(self, query: str, *args: Any) -> str:
        """Record an execute call and return a fixed success marker."""
        self.calls.append((query, args))
        return "EXECUTE_OK"

    async def fetchval(self, query: str, *args: Any) -> Any:
        """Record a fetchval call and pop pre-seeded values in FIFO order."""
        self.calls.append((query, args))
        if self.fetchval_results:
            return self.fetchval_results.pop(0)
        return 1

    async def fetchrow(self, query: str, *args: Any) -> Any:
        """Record a fetchrow call and pop pre-seeded rows in FIFO order."""
        self.calls.append((query, args))
        if self.fetchrow_results:
            return self.fetchrow_results.pop(0)
        return None


class _Acquire:
    """Simple async context manager returned by the fake pool's acquire method."""

    def __init__(self, connection: _FakeConnection) -> None:
        self.connection = connection

    async def __aenter__(self) -> _FakeConnection:
        """Yield the pre-wired fake connection to mimic asyncpg pool usage."""
        return self.connection

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        """No-op context manager exit to match async context protocol."""
        return None


class _FakePool:
    """Minimal pool test double that always returns the same fake connection."""

    def __init__(self, connection: _FakeConnection) -> None:
        self.connection = connection

    def acquire(self) -> _Acquire:
        """Return an acquire context manager compatible with storage expectations."""
        return _Acquire(self.connection)


def test_load_named_sql_queries_reads_expected_sections() -> None:
    """Ensure named SQL sections are parsed from queries.sql into lookup keys.

    This verifies that the '-- name:' marker parsing can find the commands that
    the storage layer calls dynamically at runtime.
    """
    query_map = _load_named_sql_queries(Path("options_backfill/sql/queries.sql"))

    assert "create_job" in query_map
    assert "insert_task" in query_map
    assert "claim_next_task" in query_map
    assert "upsert_option_eod_row" in query_map


def test_storage_create_job_and_enqueue_tasks() -> None:
    """Validate create_job and enqueue_tasks plumbing with deterministic fake IDs.

    The test seeds fetchval responses to simulate database-generated IDs, then
    asserts that storage returns the expected job ID and inserted task count.
    """

    async def _run() -> tuple[int, int]:
        connection = _FakeConnection()
        # First value is returned by create_job; next two by insert_task calls.
        connection.fetchval_results = [123, 1, 2]
        storage = OptionsBackfillStorage(_FakePool(connection))

        config = OptionsBackfillConfig(endpoint="eod", underlying_symbol="SPY")
        job_id = await storage.create_job(config)
        inserted = await storage.enqueue_tasks(
            job_id=job_id,
            endpoint="eod",
            task_query_params=[
                {"filter[underlying_symbol]": "SPY", "filter[tradetime_eq]": "2025-01-03"},
                {"filter[underlying_symbol]": "SPY", "filter[tradetime_eq]": "2025-01-10"},
            ],
            max_retries=3,
            page_limit=1000,
            max_pages=25,
        )
        return job_id, inserted

    job_id, inserted = asyncio.run(_run())
    assert job_id == 123
    assert inserted == 2


def test_storage_claim_complete_fail_and_summarize() -> None:
    """Exercise claim/complete/fail/summarize state transitions in one flow.

    The fake row queue simulates claiming one task and later retrieving a
    summary row, letting us validate result shaping and derived API-call units.
    """

    async def _run() -> tuple[dict[str, Any] | None, Any]:
        connection = _FakeConnection()
        # Row 1: claim_next_task return. Row 2: summarize_job aggregate row.
        connection.fetchrow_results = [
            {
                "task_id": 77,
                "status": "running",
                "query_params": {"filter[underlying_symbol]": "SPY"},
            },
            {
                "job_id": 55,
                "endpoint": "eod",
                "status": "running",
                "requested_tasks": 3,
                "completed_tasks": 2,
                "failed_tasks": 1,
                "inserted_rows": 120,
                "api_requests_made": 14,
                "unresolved_tasks": [99],
            },
        ]

        storage = OptionsBackfillStorage(_FakePool(connection))
        task = await storage.claim_next_task("worker-a")
        await storage.mark_task_completed(task_id=77, rows_received=120, rows_inserted=120, api_requests_made=14)
        await storage.mark_task_failed(task_id=99, error_message="rate limited", retryable=True)
        result = await storage.summarize_job(55)
        return task, result

    task, result = asyncio.run(_run())
    assert task is not None
    assert task["task_id"] == 77
    assert result.job_id == 55
    assert result.requested_tasks == 3
    assert result.completed_tasks == 2
    assert result.failed_tasks == 1
    assert result.api_call_units_estimated == 140
    assert result.unresolved_tasks == [99]


def test_storage_upsert_rows_returns_processed_count() -> None:
    """Confirm upsert_option_eod_rows returns processed count and executes SQL.

    This test does not validate SQL semantics; it verifies that one input row
    leads to one reported processed row and that the upsert query is invoked.
    """

    async def _run() -> tuple[int, _FakeConnection]:
        connection = _FakeConnection()
        storage = OptionsBackfillStorage(_FakePool(connection))
        count = await storage.upsert_option_eod_rows(
            [
                {
                    "contract": "SPY250117P00450000",
                    "tradetime": "2025-01-03",
                    "underlying_symbol": "SPY",
                    "exp_date": "2025-01-17",
                    "option_type": "put",
                    "strike": 450.0,
                    "bid": 1.1,
                    "ask": 1.3,
                    "midpoint": 1.2,
                }
            ],
            store_raw_json=True,
        )
        return count, connection

    count, connection = asyncio.run(_run())
    assert count == 1
    # Ensure the expected upsert statement path was reached.
    assert any("INSERT INTO options_data.option_eod" in call[0] for call in connection.calls)
