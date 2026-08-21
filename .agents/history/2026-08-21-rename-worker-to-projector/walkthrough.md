# Walkthrough: Rename `worker` to `projector` & Upgrade to `store` v0.2.0

## Overview

We have migrated the repository from `github.com/eventsalsa/worker` to `github.com/eventsalsa/projector`, upgraded to `github.com/eventsalsa/store` v0.2.0, introduced the native `Projection` interface with composable filtering decorators (`FilterStreamTypes`, `FilterEventTypes`), and aligned all PostgreSQL schema and cluster topology semantics with the `projector_*` / `projection_*` and `instance_id` conventions.

---

## Key Changes

### 1. Module & Core Types
- **Module Path**: Changed module from `github.com/eventsalsa/worker` to `github.com/eventsalsa/projector` in `go.mod`.
- **Dependencies**: Bumped `github.com/eventsalsa/store` to `v0.2.0` and removed legacy `github.com/eventsalsa/store/consumer` imports.
- **Native `Projection` Interface** (`projection.go`):
  ```go
  type Projection interface {
      Name() string
      Handle(ctx context.Context, tx pgx.Tx, event store.PersistedEvent) error
  }
  ```
- **Filter Decorators** (`projection.go`):
  - `FilterStreamTypes(p Projection, streamTypes ...string) Projection`
  - `FilterEventTypes(p Projection, eventTypes ...string) Projection`
- **Daemon Orchestrator** (`daemon.go`):
  - Renamed `worker.Worker` to `projector.Daemon` (`New(...) *Daemon`).
  - Updated instance identity to `instance_id` UUID.
  - Implemented graceful shutdown, adaptive polling, leader failover (session advisory locks and table leases), assignment tracking, safe frontier calculation, and stale gap recovery with serializable retries.

### 2. Configuration & PostgreSQL Schema Alignment
- **Table Constants & Configurations** (`config.go`, `postgres/queries.go`):
  - `projector_instances` (`instance_id UUID PRIMARY KEY`, `heartbeat_at`, `created_at`, `updated_at`)
  - `projector_leader_leases` (`lease_key TEXT PRIMARY KEY`, `leader_id UUID REFERENCES projector_instances(instance_id) ON DELETE CASCADE`)
  - `projection_assignments` (`projection_name TEXT PRIMARY KEY`, `instance_id UUID REFERENCES projector_instances(instance_id) ON DELETE SET NULL`)
  - `projection_checkpoints` (`projection_name TEXT PRIMARY KEY`, `last_position BIGINT NOT NULL DEFAULT 0`)
  - `projection_gap_skips` (`id BIGSERIAL PRIMARY KEY`, `projection_name TEXT`, `instance_id UUID`, `gap_position`, `skip_to_position`, `highest_visible_position`)
- **Default Notify Channel**: Set to empty `""` (disabled) by default; explicitly required when using `DispatcherStrategyNotify`.

### 3. Subpackages
- **`postgres/`**:
  - Registration: `RegisterInstance`, `CleanupStaleInstances`, `UpdateHeartbeat`, `ListLiveInstances`, `RemoveInstance`.
  - Assignments: `EnsureProjectionsRegistered`, `GetAssignments`, `SetAssignments`, `ComputeAssignments`, `NeedsRebalance`.
  - Checkpoints: `GetCheckpoint`, `GetCheckpointForUpdate`, `SaveCheckpoint`, `EnsureCheckpointExists`.
  - Gap Skips: `RecordGapSkip`.
  - Leader Leases: `TryAcquireLease`, `ReleaseLease`, `GetLease`.
- **`dispatcher/`**: Updated documentation and logs for projections and projector daemon.
- **`migrations/` & `cmd/migrate-gen`**:
  - DDL generator creates all 5 tables with proper foreign keys, cascade constraints, and indexes.
  - CLI flags updated to `-projector-instances-table`, `-projection-assignments-table`, `-projection-checkpoints-table`, `-projection-gap-skips-table`, `-projector-leader-leases-table`.

### 4. Integration & Unit Tests
- Ported unit test suite (`daemon_test.go`, `config_test.go`, `frontier_test.go`, `projection_test.go`, `postgres/rebalance_test.go`, `migrations/generator_test.go`, `cmd/migrate-gen/main_test.go`) to test `projector.Daemon`, decorators, and new schemas.
- Ported integration test suite (`integration_test/helpers_test.go`, `integration_test/daemon_test.go`, `integration_test/scaling_test.go`) running against PostgreSQL 16 via `testcontainers-go`.

### 5. Build, Linting & CI
- `.golangci.yml`: Updated local prefix to `github.com/eventsalsa/projector`.
- `Makefile`: Updated goimports local prefix to `github.com/eventsalsa/projector`.
- `.github/workflows/ci.yml`: Updated workflow checkout and build steps for `projector`.
- `README.md`: Completely rewritten to document `projector.Daemon`, `projector.Projection`, decorators, configuration options, table schemas, and architecture.

---

## Verification Results

### 1. Linter (`rtk make lint`)
```text
golangci-lint run --timeout=5m
0 issues.
```

### 2. Unit Tests with Race Detection (`rtk make test-unit`)
```text
go test -v -race -coverprofile=coverage.out ./...
109 tests passed across all packages.
```

### 3. Integration Tests with PostgreSQL Testcontainers (`rtk make test-integration`)
```text
go test -p 1 -v -tags=integration ./...
All integration tests passed (scale up/down, failover, lease recovery, split-brain isolation, advisory lock crash, gap skips, notify dispatcher reconnection).
```

### 4. Full Verification Suite (`rtk make check`)
Passed cleanly with code 0.
