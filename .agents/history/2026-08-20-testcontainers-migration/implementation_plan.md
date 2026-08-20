# Plan: Migrate Integration Tests to Testcontainers-Go & Fix Flaky Test

## Goal Description
Migrate the integration test suite from manual `docker compose`-managed PostgreSQL instances to automated, ephemeral containers managed via `testcontainers-go` (`github.com/testcontainers/testcontainers-go/modules/postgres`). Simultaneously, fix the flakiness/failure in `TestWorker_SplitBrain_OwnershipLost` observed in CI run [32412741032](https://github.com/eventsalsa/worker/actions/runs/32412741032/job/96567095522).

---

## Root Cause Analysis: Failing `TestWorker_SplitBrain_OwnershipLost`

In CI run [32412741032](https://github.com/eventsalsa/worker/actions/runs/32412741032/job/96567095522), `TestWorker_SplitBrain_OwnershipLost` failed with:
```text
worker_test.go:1079: condition not met within 12s: consumer not assigned to worker-1 yet
```

### The Bug
1. `TestWorker_SplitBrain_OwnershipLost` starts `worker2` with no consumers (`nil`) and `worker1` with 1 consumer (`consumer-split-brain`).
2. Both workers register as active nodes in `worker_nodes`.
3. The cluster leader executes `ComputeAssignments(consumerNames, workerIDs)`. `ComputeAssignments` sorts `workerIDs` lexicographically (`sort.Slice(sortedWorkers, ...)`) and distributes consumers in round-robin order.
4. Because worker UUIDs are randomly generated via `uuid.New()`, ~50% of the time `worker2.ID().String() < worker1.ID().String()`.
5. When `worker2` sorts first, `consumer-split-brain` is assigned to `worker2` instead of `worker1`. Since `worker2` has no instance of that consumer in memory, the consumer is never processed and `assignments[0].WorkerID` never equals `worker1.ID()`, causing a 12-second timeout failure.

### The Fix
Modify `TestWorker_SplitBrain_OwnershipLost` so that `worker1` starts alone initially without a second active worker competing in `worker_nodes`. Once `worker1` owns the consumer and processes the initial event, simulate the split-brain / ownership loss by removing `worker1`'s node row from `worker_nodes` and reassigning the consumer in `consumer_assignments` to a second worker ID (`uuid.New()`), then assert that `worker1` aborts event processing on subsequent events.

---

## User Review Required

> [!NOTE]
> `testcontainers-go` will manage the PostgreSQL container lifecycle automatically in `TestMain` within `integration_test/helpers_test.go`. Developers and CI will no longer need to run `docker compose up -d` or maintain background database instances before running `make test-integration` or `make check`.

> [!IMPORTANT]
> `docker-compose.yml` will be deleted as it is completely superseded by `testcontainers-go`.

---

## Proposed Changes

### Dependencies (`go.mod`)

#### [MODIFY] [go.mod](go.mod)
- Add `github.com/testcontainers/testcontainers-go` and `github.com/testcontainers/testcontainers-go/modules/postgres` as dependencies.

---

### Integration Test Suite (`integration_test`)

#### [MODIFY] [integration_test/helpers_test.go](integration_test/helpers_test.go)
- Add `TestMain(m *testing.M)`:
  - Check if `TESTCONTAINERS=false` (or external `POSTGRES_HOST` specified) for optional bypass; otherwise, spin up a `postgres:16-alpine` container using `testcontainers-go/modules/postgres`.
  - Configure `WithDatabase("eventsalsa_worker_test")`, `WithUsername("postgres")`, `WithPassword("postgres")`, and wait strategy (`wait.ForListeningPort()`).
  - Retrieve the dynamically assigned host, mapped port, and connection string.
  - Set the `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB` environment variables so all helper functions and tests connect seamlessly.
  - Execute `code := m.Run()`.
  - Terminate the container in deferred cleanup.
  - Call `os.Exit(code)`.
- Update `openTestDB(t)` to dynamically connect to the container settings.

#### [MODIFY] [integration_test/worker_test.go](integration_test/worker_test.go)
- Refactor `TestWorker_SplitBrain_OwnershipLost`:
  - Start only `worker1` with `consumer-split-brain`.
  - Await deterministic assignment to `worker1`.
  - Append initial event and verify processing.
  - Remove `worker1`'s registration from `worker_nodes` and reassign `consumer-split-brain` to a new `otherWorkerID := uuid.New()`.
  - Append a second event and verify `worker1` drops processing and does not advance checkpoint or write read model rows.
  - Cleanly stop `worker1`.

---

### Repository Configuration & Automation

#### [DELETE] [docker-compose.yml](docker-compose.yml)
- Delete `docker-compose.yml`.

#### [MODIFY] [Makefile](Makefile)
- Update `test-integration`: `go test -p 1 -v -tags=integration ./...`
- Update `test-integration-local`: Alias to `test-integration` or remove manual `docker compose` commands.
- Update `check` target: `lint test-unit test-integration`.

#### [MODIFY] [.github/workflows/ci.yml](.github/workflows/ci.yml)
- In the `integration-tests` job:
  - Remove `services: postgres` configuration (Testcontainers manages the container inside Docker on the runner).
  - Remove static `POSTGRES_*` environment variables from the test step.
  - Run `go test -p 1 -v -race -tags=integration -timeout=300s ./integration_test/...`.

#### [MODIFY] [README.md](README.md)
- Remove `docker compose up -d` instructions.
- Update the testing guide to document that `make test-integration` and `make check` automatically spin up PostgreSQL via `testcontainers-go`.

---

## Verification Plan

### Automated Tests
1. **Branch Verification Check**:
   - Run `rtk make check`
   - This executes:
     - `golangci-lint run --timeout=5m`
     - `go test -v -race -coverprofile=coverage.out ./...` (unit tests)
     - `go test -p 1 -v -tags=integration ./...` (integration tests with testcontainers)

2. **Flakiness Verification**:
   - Run `go test -p 1 -v -race -tags=integration -count=5 ./integration_test/...` to ensure all 21 integration tests (including `TestWorker_SplitBrain_OwnershipLost`) pass repeatedly without any race conditions or flakiness.

3. **CI Emulation**:
   - Verify that running tests without setting any `POSTGRES_*` environment variables boots the testcontainer, executes all migrations, and tears down cleanly.
