## 1. Milestone 0 - Scope Lock And Interfaces
- [x] 1.1 Confirm v1 endpoint priority is eod first and contracts second.
- [x] 1.2 Finalize v1 layout direction (single-file bootstrap vs split package target).
- [x] 1.3 Freeze OptionsBackfillConfig shape in the design spec.
- [x] 1.4 Freeze OptionsBackfillResult shape in the design spec.
- [x] 1.5 Resolve or defer all blocking design decisions so implementation can begin.

## 2. Milestone 1 - Database Foundation
- [x] 2.1 Add options_data schema and core tables (underlying_symbols, contracts, option_eod, backfill_jobs, backfill_tasks).
- [x] 2.2 Add required indexes for chain lookup and task claiming.
- [x] 2.3 Add SQL query contract for create job, enqueue tasks, claim task, and status updates.
- [x] 2.4 Implement idempotent option_eod upsert behavior.
- [x] 2.5 Implement storage helper methods for create, enqueue, claim, and complete state transitions.
- [x] 2.6 Add tests proving idempotent upsert and at least one create->claim->complete task flow.

## 3. Validation
- [x] 3.0 Confirm proposal, design, and tasks remain aligned with docs/eodhd_options_backfill_downloader_design.md Milestones 0-1.
- [x] 3.1 Run openspec validate add-eodhd-backfill-m0-m1 --strict.
- [X] 3.2 Share change for review and approval before implementation beyond Milestone 1.
