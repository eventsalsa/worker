# Walkthrough - Pluggable Observer (Metrics & Lifecycle Telemetry) in `eventsalsa/projector`

Introduced a zero-allocation, pluggable `Observer` interface into `eventsalsa/projector` configured via `projector.WithObserver(observer Observer)`. This allows applications to capture real-time projection metrics and internal daemon lifecycle telemetry without running redundant out-of-band polling queries against PostgreSQL.

---

## Changes Made

### 1. Telemetry API & Utilities (`observer.go`)
- Defined the [`Observer`](observer.go) interface:
  - `OnBatchProcessed(ctx, BatchStats)`
  - `OnHeartbeat(ctx, DaemonStats)`
  - `OnGapDetected(ctx, GapStats)`
  - `OnGapSkipped(ctx, GapStats)`
  - `OnRebalance(ctx, map[string]uuid.UUID)`
- Created immutable telemetry structures:
  - [`BatchStats`](observer.go): `ProjectionName`, `StartPosition`, `LastPosition`, `HeadPosition`, `Lag`, `EventsRead`, `EventsHandled`, `Duration`, `StaleSkipped`, `Error`.
  - [`DaemonStats`](observer.go): `InstanceID`, `IsLeader`.
  - [`GapStats`](observer.go): `ProjectionName`, `GapPosition`, `HighestVisible`, `StaleFor`.
- Implemented [`NoopObserver`](observer.go) for struct embedding.
- Implemented [`MultiObserver`](observer.go) for composite fan-out with nil stripping and nested unnesting.

### 2. Configuration (`config.go`)
- Added `Observer Observer` to [`Config`](config.go).
- Added [`WithObserver(observer Observer) Option`](config.go).

### 3. Daemon Instrumentation (`daemon.go`)
- Guarded all observer invocations with `if d.config.Observer != nil` to ensure **zero allocations** and zero clock/slice overhead when disabled.
- Emitted `OnBatchProcessed` on every batch attempt (with duration, lag calculation, event counts, and error tracking).
- Emitted `OnHeartbeat` on successful heartbeat refresh with instance ID and leadership state.
- Emitted `OnGapDetected` when a missing sequence position stalls progress.
- Emitted `OnGapSkipped` when safe-harbor advancement skips an unresolvable stale gap.
- Emitted `OnRebalance` on partition assignment commit with assignment topology.

### 4. Documentation & Examples
- Updated [`README.md`](README.md) with an **Observability and telemetry** section, metrics naming reference, Prometheus recipe, and options table.
- Added runnable [`examples/telemetry/main.go`](examples/telemetry/main.go) combining structured logging (`slog`) and metrics via `MultiObserver`.

### 5. Test Suites
- Added [`observer_test.go`](observer_test.go): testing `NoopObserver`, embedding, and `MultiObserver` fanout.
- Added unit tests in [`daemon_test.go`](daemon_test.go): verifying `BatchStats`, lag math, filter handling, handler error capture, probe errors, commit errors, gap detection, stale gap skipping, heartbeats, and nil-safety.
- Added integration tests in [`integration_test/observer_test.go`](integration_test/observer_test.go): testing full end-to-end lifecycle, backlog catchup lag, real gap detection, stale skips, and batch error recovery against PostgreSQL via testcontainers.

---

## Verification Results

### Automated Checks
All checks passed cleanly:
```bash
$ rtk make check
golangci-lint run --timeout=5m
0 issues.
go test -v -race -coverprofile=coverage.out ./...
PASS: all unit tests
go test -p 1 -v -tags=integration ./...
PASS: all integration tests (real PostgreSQL via testcontainers)
```

```bash
$ go build -v ./examples/...
github.com/eventsalsa/projector/examples/telemetry
```
