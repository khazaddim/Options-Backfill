from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class OptionsBackfillConfig:
    endpoint: str
    underlying_symbol: str | None = None
    contract: str | None = None
    option_type: str | None = None
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
