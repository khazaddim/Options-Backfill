from __future__ import annotations

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
