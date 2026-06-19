from __future__ import annotations

"""Integration tests for options_backfill storage against real PostgreSQL.

This module connects to test_database, resets the options_data schema, and
verifies end-to-end SQL behavior including schema initialization, task state
transitions, summarize rollups, and idempotent option_eod upserts.

Compared to tests/test_options_backfill_storage_unit.py, these tests validate
actual database effects rather than mocked call flow.
"""

import asyncio
import json
import os
from datetime import date
from pathlib import Path
from typing import Any

import asyncpg

from options_backfill.config import OptionsBackfillConfig
from options_backfill.storage import OptionsBackfillStorage

DEFAULT_PG_CONFIG_PATH = Path(r"C:\Users\khazy\OneDrive\Documents\keys\PG\pg_config.json")


def _load_pg_config() -> dict[str, Any]:
    """Load and validate integration-test PostgreSQL connection settings.

    Configuration can be overridden via PG_TEST_CONFIG_PATH and is restricted
    to database='test_database' as a safeguard against accidental production use.
    """
    config_path = Path(os.getenv("PG_TEST_CONFIG_PATH", str(DEFAULT_PG_CONFIG_PATH)))
    if not config_path.exists():
        raise FileNotFoundError(
            f"Postgres test config was not found at {config_path}. "
            "Set PG_TEST_CONFIG_PATH to a valid config file."
        )

    config = json.loads(config_path.read_text(encoding="utf-8"))
    required_keys = {"username", "password", "host", "port", "database"}
    missing = required_keys.difference(config)
    if missing:
        missing_keys = ", ".join(sorted(missing))
        raise ValueError(f"Postgres test config is missing required keys: {missing_keys}")

    if str(config["database"]) != "test_database":
        raise ValueError("Integration tests only run against database='test_database'.")

    return config


async def _new_pool() -> asyncpg.Pool:
    """Create a small asyncpg pool for integration tests."""
    config = _load_pg_config()
    return await asyncpg.create_pool(
        user=str(config["username"]),
        password=str(config["password"]),
        host=str(config["host"]),
        port=int(config["port"]),
        database=str(config["database"]),
        min_size=1,
        max_size=4,
    )


async def _reset_schema(pool: asyncpg.Pool) -> None:
    """Reset the options_data schema to make each test fully isolated."""
    await pool.execute("DROP SCHEMA IF EXISTS options_data CASCADE;")


def test_storage_real_db_create_enqueue_claim_complete_and_summarize() -> None:
    """Run a full DB-backed task lifecycle and validate summary rollups.

    This test covers schema initialization, job/task creation, claiming, status
    transitions, and summarize_job output against a real PostgreSQL instance.
    """

    async def _run() -> None:
        pool = await _new_pool()
        try:
            # Start from a clean schema so counts and status transitions are deterministic.
            await _reset_schema(pool)
            storage = OptionsBackfillStorage(pool)
            await storage.initialize_schema()

            config = OptionsBackfillConfig(
                endpoint="eod",
                underlying_symbol="SPY",
                option_type="put",
                tradetime_from="2025-01-01",
                tradetime_to="2025-01-10",
            )
            job_id = await storage.create_job(config)
            inserted_tasks = await storage.enqueue_tasks(
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
            assert inserted_tasks == 2

            # Claim first pending task and record successful completion counters.
            claimed_task = await storage.claim_next_task("worker-real")
            assert claimed_task is not None

            await storage.mark_task_completed(
                task_id=int(claimed_task["task_id"]),
                rows_received=50,
                rows_inserted=45,
                api_requests_made=3,
            )

            # Mark one pending task as failed to validate summary counters.
            second_task = await storage.claim_next_task("worker-real")
            assert second_task is not None
            await storage.mark_task_failed(task_id=int(second_task["task_id"]), error_message="forced", retryable=False)

            await storage.set_job_status(job_id, "running")
            result = await storage.summarize_job(job_id)

            assert result.job_id == job_id
            assert result.requested_tasks == 2
            assert result.completed_tasks == 1
            assert result.failed_tasks == 1
            assert result.api_requests_made == 3
            assert result.api_call_units_estimated == 30
        finally:
            await pool.close()

    asyncio.run(_run())


def test_storage_real_db_upsert_option_eod_is_idempotent() -> None:
    """Verify option_eod upsert remains single-row idempotent by primary key.

    The same (contract, tradetime) key is inserted twice with updated prices.
    Expected behavior is one physical row with latest bid/ask/midpoint values.
    """

    async def _run() -> None:
        pool = await _new_pool()
        try:
            await _reset_schema(pool)
            storage = OptionsBackfillStorage(pool)
            await storage.initialize_schema()

            row = {
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

            updated_row = {**row, "bid": 1.9, "ask": 2.1, "midpoint": 2.0}

            inserted_first = await storage.upsert_option_eod_rows([row], store_raw_json=True)
            inserted_second = await storage.upsert_option_eod_rows([updated_row], store_raw_json=True)

            assert inserted_first == 1
            assert inserted_second == 1

            async with pool.acquire() as connection:
                # Row count proves no duplicates; selected columns prove update policy.
                count = await connection.fetchval("SELECT COUNT(*) FROM options_data.option_eod")
                saved = await connection.fetchrow(
                    "SELECT bid, ask, midpoint FROM options_data.option_eod WHERE contract = $1 AND tradetime = $2",
                    row["contract"],
                    date.fromisoformat(str(row["tradetime"])),
                )

            assert int(count) == 1
            assert float(saved["bid"]) == 1.9
            assert float(saved["ask"]) == 2.1
            assert float(saved["midpoint"]) == 2.0
        finally:
            await pool.close()

    asyncio.run(_run())
