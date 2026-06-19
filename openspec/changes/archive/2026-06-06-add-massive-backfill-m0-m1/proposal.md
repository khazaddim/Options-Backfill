# Change: Add Massive backfill milestone 0 and 1 foundation

## Why
The Massive options backfill effort needs a formal scope freeze and durable database foundation before async network orchestration begins. Capturing Milestones 0 and 1 as an OpenSpec change improves reviewability, reduces implementation drift, and creates explicit acceptance gates.

## What Changes
- Freeze v1 scope and interfaces for the backfill module.
- Lock endpoint priority to eod first and contracts second.
- Lock initial module layout direction and the v1 shapes for OptionsBackfillConfig and OptionsBackfillResult.
- Define and implement the PostgreSQL options_data schema foundation.
- Define task-queue persistence primitives needed for create, enqueue, claim, and complete flows.
- Define idempotent option_eod upsert semantics for safe retries and restarts.

## Impact
- Affected specs: options-backfill (new capability)
- Affected docs: docs/massive_options_backfill_downloader_design.md
- Affected code (Milestone 1 target): options_backfill/sql/schema.sql, options_backfill/sql/queries.sql, storage layer module(s)
- Out of scope: async provider fetching, retry engine, and worker orchestration beyond DB claim lifecycle

## Roadmap Alignment
- Source roadmap: docs/massive_options_backfill_downloader_design.md
- Covered milestones: Milestone 0 and Milestone 1

