# options-backfill Specification

## Purpose
Provide the durable foundation for Massive options backfill work, including the frozen Milestone 0 interfaces and the Milestone 1 PostgreSQL storage primitives for jobs, tasks, and idempotent EOD row persistence.
## Requirements
### Requirement: Milestone 0 scope and interfaces SHALL be frozen before implementation
The project SHALL complete a formal scope and interface freeze for v1 before implementation beyond documentation and planning begins.

#### Scenario: Endpoint priority is explicitly frozen
- **WHEN** Milestone 0 is reviewed
- **THEN** the specification records eod as first endpoint priority
- **AND** the specification records contracts as second endpoint priority

#### Scenario: Interface shapes are explicitly frozen
- **WHEN** Milestone 0 is reviewed
- **THEN** the specification includes final field shapes for OptionsBackfillConfig and OptionsBackfillResult
- **AND** no unresolved blocking design decisions remain for Milestone 1 start

### Requirement: Milestone 1 database foundation SHALL provide durable backfill state
The system SHALL provide a PostgreSQL persistence foundation for options backfill job state, task state, and idempotent EOD row storage.

#### Scenario: Durable schema exists for jobs, tasks, and EOD rows
- **WHEN** Milestone 1 database setup is applied
- **THEN** options_data schema and core tables exist for option_eod, contracts, underlying_symbols, backfill_jobs, and backfill_tasks
- **AND** indexes exist for chain lookups and task claiming

#### Scenario: Task lifecycle can be persisted and advanced
- **WHEN** a backfill job is created and tasks are enqueued
- **THEN** at least one task can transition from pending to running through claim-safe semantics
- **AND** the task can be marked completed with persisted counters

#### Scenario: EOD row writes are idempotent
- **WHEN** the same contract and tradetime row is inserted more than once
- **THEN** persistence does not create duplicate primary-key rows
- **AND** update semantics follow the defined upsert policy


