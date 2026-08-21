# Implementation Plan: Rename `worker` to `projector` & Upgrade to `store` v0.2.0

Migrate the codebase from `github.com/eventsalsa/worker` to `github.com/eventsalsa/projector`, upgrade to `github.com/eventsalsa/store` v0.2.0, introduce the native `projector.Projection` interface with composable filtering decorators (`FilterStreamTypes`, `FilterEventTypes`), and adopt clean `instance_id` / `projector_*` / `projection_*` PostgreSQL schema and cluster topology semantics.

---

## User Review Required

> [!IMPORTANT]
> **Breaking Changes Summary**:
> - **Go Module**: `github.com/eventsalsa/worker` $\rightarrow$ `github.com/eventsalsa/projector`.
> - **Core Type**: `worker.Worker` $\rightarrow$ `projector.Daemon` (`projector.New(...) *Daemon`).
> - **Native Projection Interface**: Defined in `package projector` as `Projection` with `Name() string` and `Handle(ctx context.Context, tx pgx.Tx, event store.PersistedEvent) error`.
> - **Decorators**: Stream scoping is replaced by `FilterStreamTypes(p, ...)` and `FilterEventTypes(p, ...)`.
> - **PostgreSQL Table Names**:
>   - `worker_nodes` $\rightarrow$ `projector_instances` (PK: `instance_id UUID`)
>   - `worker_leader_election` $\rightarrow$ `projector_leader_leases` (FK: `leader_instance_id UUID`)
>   - `consumer_assignments` $\rightarrow$ `projection_assignments` (`projection_name`, `instance_id UUID`)
>   - `consumer_checkpoints` $\rightarrow$ `projection_checkpoints` (`projection_name`, `last_position BIGINT`)
>   - `consumer_gap_skips` $\rightarrow$ `projection_gap_skips` (`projection_name`, `instance_id UUID`)
> - **Notify Channel**: Default is empty `""` (disabled). When `DispatcherStrategyNotify` is selected, `WithNotifyChannel` must be explicitly provided.

---

## Architecture & Ecosystem Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    eventsalsa/store v0.2.0                  │
│       (Append-Only Stream Persistence & Concurrency)        │
└──────────────┬───────────────────────────────┬──────────────┘
               │                               │
               ▼                               ▼
┌──────────────────────────────┐ ┌────────────────────────────┐
│    eventsalsa/projector      │ │     eventsalsa/outbox      │
│  - Native Projection & Filter│ │  - Outbox Table Polling    │
│  - Distributed Daemon        │ │  - Message Broker Relays   │
│  - Checkpoints & Leases      │ └────────────────────────────┘
│  - Gap & Frontier Detection  │
└──────────────────────────────┘
```

---

## Proposed Phases

### Phase 1: Go Module Rename, Native `Projection` Interface & Core Daemon
1. **Branching**: Create branch `feat/rename-to-projector` from `main`.
2. **`go.mod` / `go.sum`**:
   - Change module to `github.com/eventsalsa/projector`.
   - Bump `github.com/eventsalsa/store` to `v0.2.0`.
   - Remove `github.com/eventsalsa/store/consumer` imports.
3. **`projection.go`** (NEW):
   - Define `Projection` interface:
     ```go
     type Projection interface {
         Name() string
         Handle(ctx context.Context, tx pgx.Tx, event store.PersistedEvent) error
     }
     ```
   - Implement decorators:
     - `FilterStreamTypes(p Projection, streamTypes ...string) Projection`
     - `FilterEventTypes(p Projection, eventTypes ...string) Projection`
4. **`daemon.go`** (renamed from `worker.go`):
   - Rename `Worker` struct to `Daemon`.
   - Rename constructor to `New(db PgxPool, eventStore projectorStore, projections []Projection, opts ...Option) *Daemon`.
   - Change node identity to `instance_id` / `d.id` (UUID).
   - Update error variables: `ErrNilDB`, `ErrNilStore`, `ErrNilDispatcher`, `ErrAlreadyStarted`, `ErrMissingNotifyConnectionString`, `ErrMissingNotifyChannel`, `ErrConsecutiveFailures`.
   - Update receiver methods `(d *Daemon)`.
5. **`config.go`**:
   - `ProjectorInstancesTable` (default: `"projector_instances"`)
   - `ProjectorLeaderLeasesTable` (default: `"projector_leader_leases"`)
   - `ProjectionAssignmentsTable` (default: `"projection_assignments"`)
   - `ProjectionCheckpointsTable` (default: `"projection_checkpoints"`)
   - `ProjectionGapSkipsTable` (default: `"projection_gap_skips"`)
   - `NotifyChannel` default: `""` (empty/disabled).
   - Matching `With*` options.
6. **`daemon_test.go`** (renamed from `worker_test.go`) & **`frontier_test.go`**:
   - Update all mock projections and test suites to use `Daemon` and `Projection`.
7. **Phase 1 Gate**: Run `rtk make test-unit`, commit with conventional commit format, push branch, and open PR.

---

### Phase 2: Subpackages (`postgres/`, `dispatcher/`, `migrations/`, `cmd/migrate-gen/`)
1. **`postgres/` subpackage**:
   - Update `postgres/queries.go` constants:
     - `DefaultProjectorInstancesTable = "projector_instances"`
     - `DefaultProjectorLeaderLeasesTable = "projector_leader_leases"`
     - `DefaultProjectionAssignmentsTable = "projection_assignments"`
     - `DefaultProjectionCheckpointsTable = "projection_checkpoints"`
     - `DefaultProjectionGapSkipsTable = "projection_gap_skips"`
   - Update `postgres/registration.go`:
     - `RegisterInstance`, `CleanupStaleInstances`, `UpdateHeartbeat`, `ListLiveInstances`, `RemoveInstance`.
     - `ErrInstanceRegistrationMissing`.
   - Update `postgres/assignment.go`:
     - `ProjectionAssignment` (`ProjectionName string`, `InstanceID uuid.UUID`, `Assigned bool`).
     - `EnsureProjectionsRegistered`, `GetAssignments`, `SetAssignments`.
   - Update `postgres/checkpoint.go`:
     - `GetCheckpoint`, `GetCheckpointForUpdate`, `SaveCheckpoint`, `EnsureCheckpointExists` with `projectionName`.
   - Update `postgres/gap_skip.go`:
     - `ProjectionGapSkipRecord` (`ProjectionName`, `InstanceID`, `GapPosition`, `SkipToPosition`, `HighestVisiblePosition`, `FirstSeenAt`).
     - `RecordGapSkip`.
   - Update `postgres/lease.go`:
     - `TryAcquireLease`, `ReleaseLease`, `GetLease` with `instanceID`.
   - Update `postgres/rebalance.go` & `postgres/rebalance_test.go`:
     - `ComputeAssignments(projectionNames []string, instanceIDs []uuid.UUID)`
     - `NeedsRebalance(current []ProjectionAssignment, liveInstances []uuid.UUID)`
2. **`dispatcher/` subpackage**:
   - Update documentation and log messages to reference projections and the projector daemon.
3. **`migrations/` subpackage & `cmd/migrate-gen`**:
   - `migrations/generator.go`:
     - Default output filename: `%s_init_projector_infrastructure.sql`.
     - DDL updated to generate `projector_instances`, `projection_assignments`, `projection_checkpoints`, `projection_gap_skips`, `projector_leader_leases`.
   - `cmd/migrate-gen/main.go`:
     - Flags: `-instances-table`, `-assignments-table`, `-checkpoints-table`, `-gap-skips-table`, `-leader-leases-table`.
4. **Phase 2 Gate**: Run `rtk make test-unit` & `rtk make lint`, commit with conventional commit format, push branch.

---

### Phase 3: Integration Tests, Tooling, CI/CD, Documentation & Agent Skills
1. **`integration_test/`**:
   - Rename `worker_test.go` to `daemon_test.go`.
   - Update `helpers_test.go` and `scaling_test.go`:
     - Testcontainer database: `postgres.WithDatabase("eventsalsa_projector_test")`.
     - Package aliases: `projectorpkg`, `projectormigrations`, `projectorpostgres`.
     - Update test projections (`testProjection`) to implement `projector.Projection` (`Handle(...)`).
     - Add integration tests verifying `FilterStreamTypes` and `FilterEventTypes` decorators.
2. **Build, Formatting & CI/CD**:
   - `Makefile`: `goimports -w -local github.com/eventsalsa/projector .`.
   - `.github/workflows/ci.yml`: Update paths and names (`projector`).
   - `release-please-config.json` & `.release-please-manifest.json`.
3. **Documentation & Agent Guidelines**:
   - `README.md`: Rewrite with `projector.Daemon`, `projector.Projection`, decorators, and `instance_id`.
   - `doc.go`: Package documentation.
   - `AGENTS.md`: Update repository name and skills references.
   - `.agents/skills/event-sourcing-worker/`: Rename/update terminology.
4. **Phase 3 Gate**: Run `rtk make check` (`lint`, `test-unit`, `test-integration`), commit with conventional commit format, push branch, archive task history.

---

## Verification Plan

### Automated Tests
- **Unit tests**:
  ```bash
  rtk make test-unit
  ```
- **Linting & static analysis**:
  ```bash
  rtk make lint
  ```
- **Integration tests with Testcontainers**:
  ```bash
  rtk make test-integration
  ```
- **Full Verification Suite**:
  ```bash
  rtk make check
  ```

### Manual Verification
- Verify that `go.mod` has clean imports with `eventsalsa/store v0.2.0`.
- Verify generated SQL migration runs cleanly on PostgreSQL 16.
- Verify that all CI workflow actions match the new repository path.
