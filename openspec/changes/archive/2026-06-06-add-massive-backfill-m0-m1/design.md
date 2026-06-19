## Context
This change formalizes the first two milestones of the Massive options backfill plan.

Milestone 0 is a design gate: freeze scope, interfaces, and priority so implementation can proceed in small reviewable PRs.
Milestone 1 is a persistence gate: establish PostgreSQL as the durable source of truth for backfill state before introducing async provider orchestration.

## Goals / Non-Goals
- Goals:
  - Freeze v1 endpoint order, interface shapes, and module layout direction.
  - Define database schema, indexes, and core query contract for jobs/tasks/eod rows.
  - Ensure idempotent writes and claim-safe task lifecycle transitions.
- Non-Goals:
  - Implementing aiohttp provider fetch paths.
  - Implementing retry policy and process-local rate limiter.
  - Implementing end-to-end multi-worker orchestration.

## Decisions
- Decision: Combine Milestones 0 and 1 in one OpenSpec change.
  - Rationale: Milestone 1 depends directly on Milestone 0 contract stability.
  - Alternative considered: Separate changes per milestone. Rejected for higher coordination overhead with little risk isolation benefit at this stage.

- Decision: Prioritize eod endpoint in v1 and treat contracts as second endpoint.
  - Rationale: eod records are the direct dependency for strategy premium reconstruction.

- Decision: Require options_data schema with backfill_jobs and backfill_tasks queue primitives before network orchestration.
  - Rationale: Durable state and idempotent writes are prerequisites for safe retries and resumability.

- Decision: Treat this as a design plus DB foundation scope only.
  - Rationale: Keeps this change reviewable and avoids pulling in async behavior risk early.

## Risks / Trade-offs
- Risk: Interface freeze may need revisions when provider implementation starts.
  - Mitigation: Keep explicit open-questions list in design doc and allow follow-up change request.

- Risk: Database-first approach may introduce SQL churn before provider code exists.
  - Mitigation: Keep SQL contract aligned with strict milestone acceptance criteria and avoid speculative columns.

## Migration Plan
1. Approve this change request.
2. Apply Milestone 0 updates in design docs and freeze interface wording.
3. Implement Milestone 1 schema and storage primitives in code.
4. Validate idempotent upsert and task claim lifecycle with tests.
5. Move to next change for async provider and safety controls.

## Open Questions
- Should raw JSON storage default remain disabled except targeted debug runs?
- Should option_eod conflict policy update selected fields or preserve first-seen values only?
- Should initial planning granularity target daily tradetime slices by default?

