# eventsalsa/worker

[![CI](https://github.com/eventsalsa/worker/actions/workflows/ci.yml/badge.svg)](https://github.com/eventsalsa/worker/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/eventsalsa/worker.svg)](https://pkg.go.dev/github.com/eventsalsa/worker)

`github.com/eventsalsa/worker` is a horizontally scalable, PostgreSQL-native consumer processing module for event-sourced systems.

It builds on [`github.com/eventsalsa/store`](https://github.com/eventsalsa/store) and adds worker coordination, leader election, consumer assignment, checkpointing, wakeup dispatching, and batched transactional event processing with no external coordination service.

## Features

- **Worker orchestrator** for starting, coordinating, and stopping consumer goroutines
- **Pluggable leader election**: choose between PostgreSQL session-level advisory locks (`pg_try_advisory_lock`) or a PgBouncer-safe table lease heartbeat strategy
- **Horizontal scaling** through round-robin consumer assignment across active workers
- **Gap-aware checkpointing**: probe the frontier, handle only safe rows, and audit stale-gap advances
- **Adaptive polling** with exponential backoff and low-latency wakeups
- **Wakeup dispatchers**:
  - polling via periodic latest-position checks
  - PostgreSQL `LISTEN`/`NOTIFY` with reconciliation polling fallback
- **Migration generation** for worker infrastructure tables
- **Customizable configuration** via the functional options pattern

## How it works

At runtime, each worker instance:

1. Performs a best-effort cleanup of very stale worker registrations, then registers itself in PostgreSQL and updates its heartbeat periodically.
2. Starts a dispatcher that detects newly appended events.
3. Participates in leader election (using advisory locks or database-backed leases).
4. Lets the elected leader rebalance consumer assignments across live workers.
5. Runs consumer goroutines only for the consumers assigned to that worker.
6. Probes the global frontier outside the batch transaction, then processes only the current safe frontier inside the batch transaction.

This design keeps coordination inside PostgreSQL, making the module straightforward to operate in environments that already depend on Postgres.

## Architecture decisions

The module intentionally favors simple, database-native coordination:

- **Single leader, many workers**: only the elected leader recalculates assignments; every worker still heartbeats and processes its own assigned consumers.
- **Advisory-lock leadership**: leader election uses PostgreSQL session-level advisory locks instead of Redis, ZooKeeper, or etcd.
- **Conservative worker-node cleanup**: startup may prune `worker_nodes` rows only when they are much older than the live-worker timeout, so housekeeping stays less aggressive than rebalance liveness checks.
- **Scoped handling after frontier probe**: consumers that implement `consumer.ScopedConsumer` still receive only matching events, but checkpoint correctness comes from an unscoped frontier probe rather than from the last matching scoped row.
- **Broadcast wakeups via close-and-replace channels**: dispatchers notify all waiting consumer loops by closing the current wakeup channel and replacing it with a new one.
- **Adaptive polling**: consumer loops start at a base poll interval, back off exponentially when idle, stay hot while blocked on known gaps, and reset immediately when new events are found or a wakeup arrives.

## Package layout

```text
.
├── cmd/migrate-gen/         # Stable CLI for generating worker infrastructure migrations
├── worker.go / config.go   # Worker orchestrator and configuration
├── dispatcher/             # PollDispatcher and NotifyDispatcher
├── postgres/               # PostgreSQL DAL for registration, leadership, assignment, checkpoints, gap-skip audit
├── migrations/             # SQL migration generator for worker metadata tables
└── integration_test/       # Integration tests against real PostgreSQL
```

## Installation

```bash
go get github.com/eventsalsa/worker
```

The module is intended to be used alongside `github.com/eventsalsa/store` and its PostgreSQL implementation.

## Prerequisites

- Go 1.24+
- PostgreSQL 16+
- [`github.com/eventsalsa/store`](https://github.com/eventsalsa/store)
- `golangci-lint` for local linting
- Docker (optional, for local integration testing)

## Quick start

### 1. Create or open a PostgreSQL database

You need:

- the event store tables from `github.com/eventsalsa/store`
- the worker metadata tables generated from `github.com/eventsalsa/worker/cmd/migrate-gen`

### 2. Build an event store

```go
package main

import (
    "context"
    "database/sql"
    "log"
    "os"
    "os/signal"
    "syscall"
    "time"

    _ "github.com/lib/pq"
    "github.com/eventsalsa/store"
    storepostgres "github.com/eventsalsa/store/postgres"
    "github.com/eventsalsa/store/consumer"
    "github.com/eventsalsa/worker"
)

type AccountProjection struct{}

func (p *AccountProjection) Name() string {
    return "account_projection"
}

func (p *AccountProjection) AggregateTypes() []string {
    return []string{"Account"}
}

func (p *AccountProjection) Handle(ctx context.Context, tx *sql.Tx, event store.PersistedEvent) error {
    _ = ctx
    _ = tx
    _ = event
    return nil
}

func main() {
    connStr := os.Getenv("DATABASE_URL")

    db, err := sql.Open("postgres", connStr)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())

    consumers := []consumer.Consumer{
        &AccountProjection{},
    }

    w := worker.New(
        db,
        eventStore,
        consumers,
        worker.WithBatchSize(100),
        worker.WithPollInterval(500*time.Millisecond),
    )

    ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer stop()

    if err := w.Start(ctx); err != nil {
        log.Fatal(err)
    }
}
```

> [!NOTE]
> `(*worker.Worker).Start` blocks until the context is canceled or a fatal runtime error occurs.

## Configuration

Workers are configured with functional options:

```go
w := worker.New(db, eventStore, consumers,
    worker.WithBatchSize(100),
    worker.WithPollInterval(500*time.Millisecond),
    worker.WithMaxPollInterval(5*time.Second),
    worker.WithDispatcherInterval(200*time.Millisecond),
    worker.WithHeartbeatInterval(5*time.Second),
    worker.WithHeartbeatTimeout(30*time.Second),
    worker.WithRebalanceInterval(5*time.Second),
    worker.WithBatchPause(200*time.Millisecond),
    worker.WithDispatcherStrategy(worker.DispatcherStrategyNotify),
    worker.WithNotifyConnectionString(connStr),
    worker.WithNotifyChannel("worker_events"),
    worker.WithLogger(myLogger),
)
```

### Available options

| Option | Description | Default |
| --- | --- | --- |
| `WithBatchSize(n int)` | Maximum size of the probed/handled batch window | `100` |
| `WithPollInterval(d time.Duration)` | Base consumer poll interval | `1s` |
| `WithMaxPollInterval(d time.Duration)` | Maximum adaptive poll backoff | `30s` |
| `WithDispatcherInterval(d time.Duration)` | Poll dispatcher interval | `200ms` |
| `WithHeartbeatInterval(d time.Duration)` | Worker heartbeat interval | `5s` |
| `WithHeartbeatTimeout(d time.Duration)` | Heartbeat staleness timeout | `30s` |
| `WithRebalanceInterval(d time.Duration)` | Leader rebalance check interval | `5s` |
| `WithBatchPause(d time.Duration)` | Pause between consecutive full batches during catch-up | `200ms` |
| `WithLogger(l store.Logger)` | Custom logger implementation | `store.NoOpLogger{}` |
| `WithWorkerNodesTable(name string)` | Override worker registration table name | `worker_nodes` |
| `WithConsumerAssignmentsTable(name string)` | Override assignment table name | `consumer_assignments` |
| `WithConsumerCheckpointsTable(name string)` | Override checkpoint table name | `consumer_checkpoints` |
| `WithConsumerGapSkipsTable(name string)` | Override stale-gap audit table name | `consumer_gap_skips` |
| `WithStaleGapThreshold(d time.Duration)` | How long the worker waits on the same missing position before safe-harbor advancement | `30s` |
| `WithStaleGapHarborLag(n int)` | How far behind the visible head the worker tries to stay when advancing past a stale gap; capped at `BatchSize-1` and further clamped to the reachable visible window during fallback | `8` |
| `WithDispatcherStrategy(strategy)` | Wakeup strategy: `worker.DispatcherStrategyPoll` or `worker.DispatcherStrategyNotify` | `worker.DispatcherStrategyPoll` |
| `WithNotifyConnectionString(connStr string)` | PostgreSQL connection string used by the notify dispatcher | empty |
| `WithNotifyChannel(channel string)` | PostgreSQL notification channel for the notify dispatcher | `worker_events` |
| `WithLeaderStrategy(strategy)` | Leader election strategy: `worker.LeaderStrategyAdvisory` or `worker.LeaderStrategyLease` | `worker.LeaderStrategyAdvisory` |
| `WithLeaderElectionTable(name string)` | Override leader election lease table name | `worker_leader_election` |

### Leader election strategies

#### Advisory lock strategy (Default)

`worker.LeaderStrategyAdvisory` uses PostgreSQL session-level advisory locks. This strategy is extremely lightweight and releases immediately when a node goes down, but it requires a dedicated, persistent connection and is **incompatible** with connection poolers like PgBouncer in transaction pooling mode.

#### Lease-based strategy (PgBouncer-safe)

`worker.LeaderStrategyLease` coordinates leadership through a central lease table (`worker_leader_election`) using short-lived transactions. The leader periodically heartbeats/renews its lease record. If a leader crashes, other nodes can take over leadership after the lease duration (`HeartbeatTimeout`) has expired. This strategy is fully safe for deployments running behind PgBouncer in transaction pooling mode.

### Dispatcher strategies

#### Poll dispatcher

`worker.DispatcherStrategyPoll` periodically checks the latest global event position and wakes consumers when it advances.

Use it when:

- you want the simplest setup
- low-latency wakeups are not critical
- you do not want an extra listener connection

#### Notify dispatcher

`worker.DispatcherStrategyNotify` listens for PostgreSQL notifications and also performs reconciliation polling as a safety net.

Use it when:

- you want lower event-to-consumer latency
- your store append path emits PostgreSQL notifications
- you can provide a dedicated notification connection string

When you use notify mode, configure both sides to use the same channel:

- the **store** appends events and emits `NOTIFY`
- the **worker** listens on that channel and wakes assigned consumers

Example:

```go
storeConfig := storepostgres.NewStoreConfig(
    storepostgres.WithNotifyChannel("worker_events"),
)

eventStore := storepostgres.NewStore(storeConfig)

w := worker.New(
    db,
    eventStore,
    consumers,
    worker.WithDispatcherStrategy(worker.DispatcherStrategyNotify),
    worker.WithNotifyConnectionString(connStr),
    worker.WithNotifyChannel("worker_events"),
)
```

## Consumer contract

The worker consumes handlers from `github.com/eventsalsa/store/consumer`.

```go
type Consumer interface {
    Name() string
    Handle(ctx context.Context, tx *sql.Tx, event store.PersistedEvent) error
}

type ScopedConsumer interface {
    Consumer
    AggregateTypes() []string
}
```

### Important consumer semantics

- Consumer names must be unique across the worker process.
- A consumer with an empty name is invalid.
- `Handle` receives a transaction that also owns checkpoint persistence.
- Consumers must **not** call `Commit` or `Rollback` on the provided transaction.
- If `Handle` returns an error, the batch fails and the checkpoint is not advanced.
- `ScopedConsumer` is optional; consumers that do not implement it receive all events.

### Checkpoint semantics

Consumer checkpoints track the **highest safe global position** the worker has advanced to, not the last matching scoped event handled by that consumer.

That means:

- scoped consumers can handle zero events in a batch while the checkpoint still advances
- later matching events never define the checkpoint target by themselves
- stale-gap advances are durably recorded in `consumer_gap_skips`

## Processing model

For each assigned consumer, the worker repeatedly:

1. loads the current checkpoint
2. performs an **unscoped frontier probe** outside the batch transaction
3. computes the current safe frontier from that probe
4. waits on unresolved gaps until they either resolve or become stale
5. when a gap is stale, advances conservatively to a safe harbor behind the current visible head, or to the earliest reachable visible frontier when the probe window is smaller than the configured lag
6. opens the batch transaction and calls `Handle` only for rows at or below the target frontier
7. saves the checkpoint target, records any stale-gap skip, and commits the transaction

That means read-model updates performed through `tx`, checkpoint moves, and stale-gap audit records stay atomic with one another.

### Stale-gap behavior

`global_position` values are sequence-backed, so a lower position can appear later than a higher committed position. The worker therefore:

- refuses to checkpoint past a visible gap immediately
- keeps polling the same gap for up to `StaleGapThreshold`
- if the gap stays unresolved, advances conservatively to a safe harbor behind the current visible head, falling back to the earliest reachable visible frontier when the probe window is smaller than the configured lag
- records that decision in `consumer_gap_skips` so operators can inspect it later

If a stale-gap decision later proves too aggressive for a consumer, the recovery path is to rewind or rebuild from a safe checkpoint.

## Migration generation

For the quickest path, use the stable `cmd/migrate-gen` entrypoint.

```bash
go run github.com/eventsalsa/worker/cmd/migrate-gen \
  -output ./db/migrations \
  -filename 002_worker_tables.sql
```

The CLI defaults match `migrations.DefaultConfig()`, and you can override table names to line up with `worker.With*Table(...)` options:

```bash
go run github.com/eventsalsa/worker/cmd/migrate-gen \
  -output ./db/migrations \
  -worker-nodes-table infra.worker_nodes \
  -consumer-assignments-table infra.consumer_assignments \
  -consumer-checkpoints-table infra.consumer_checkpoints \
  -consumer-gap-skips-table infra.consumer_gap_skips
```

For more advanced integration, use the `migrations` package directly from your own program.

```go
package main

import (
    "log"

    "github.com/eventsalsa/worker/migrations"
)

func main() {
	config := migrations.DefaultConfig()
	config.OutputFolder = "./db/migrations"
	config.OutputFilename = "002_worker_tables.sql"
	config.WorkerNodesTable = "infra.worker_nodes"
	config.ConsumerAssignmentsTable = "infra.consumer_assignments"
	config.ConsumerCheckpointsTable = "infra.consumer_checkpoints"
	config.ConsumerGapSkipsTable = "infra.consumer_gap_skips"

	if err := migrations.GeneratePostgres(&config); err != nil {
		log.Fatal(err)
	}
}
```

The generated migration creates:

- `worker_nodes`
- `consumer_assignments`
- `consumer_checkpoints`
- `consumer_gap_skips`

It also creates schemas automatically when a configured table name includes a schema prefix.

## Running multiple workers

To scale horizontally, start multiple instances of the same worker configuration against the same PostgreSQL database.

Each instance will:

- register itself with a unique worker ID
- heartbeat into `worker_nodes`
- observe leader election
- receive a subset of consumer assignments
- stop processing consumers that are reassigned elsewhere

This makes scaling operationally simple: add more worker processes and let PostgreSQL-backed assignment rebalancing distribute the consumers.

On startup, a worker may also prune `worker_nodes` rows whose heartbeats are older than twice the configured heartbeat timeout. That cleanup is best-effort and intentionally more conservative than rebalance liveness detection. If a worker ever loses its own registration row later, it shuts down instead of continuing to run invisibly.

## Development

### Clone and set up

```bash
git clone https://github.com/eventsalsa/worker.git
cd worker
go mod download
```

### Start PostgreSQL locally

```bash
docker compose up -d
```

### Common commands

```bash
make build
make test
make lint
make test-integration-local
```

### Make targets

| Target | Description |
| --- | --- |
| `make help` | Show available targets |
| `make test` | Run the default test suite |
| `make test-unit` | Run unit tests with race detection and coverage |
| `make test-integration` | Run integration tests against an existing PostgreSQL instance |
| `make test-integration-local` | Start PostgreSQL with Docker Compose, run integration tests, then tear it down |
| `make lint` | Run `golangci-lint` |
| `make fmt` | Run `gofmt` and `goimports` |
| `make build` | Build all packages |

### Integration test environment variables

`make test-integration` expects a running PostgreSQL instance and reads these variables:

- `POSTGRES_HOST` (default: `localhost`)
- `POSTGRES_PORT` (default: `5432`)
- `POSTGRES_USER` (default: `postgres`)
- `POSTGRES_PASSWORD` (default: `postgres`)
- `POSTGRES_DB` (default: `eventsalsa_worker_test`)

Example:

```bash
POSTGRES_HOST=localhost \
POSTGRES_PORT=5432 \
POSTGRES_USER=postgres \
POSTGRES_PASSWORD=postgres \
POSTGRES_DB=eventsalsa_worker_test \
make test-integration
```

## Notes

- The worker module coordinates consumers; it does not replace the event store.
- The worker depends on PostgreSQL for both persistence and coordination.
- For most applications, start with the poll dispatcher and move to notify mode when lower wakeup latency is needed.
