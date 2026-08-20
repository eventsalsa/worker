# Walkthrough: Upgrade to `eventsalsa/store` v0.1.0 & Stream Terminology Migration

## Overview

Upgraded `github.com/eventsalsa/store` to `v0.1.0` and migrated all `Aggregate`/`aggregate` terminology across `eventsalsa/worker` to `Stream`/`stream`. This aligns consumer interfaces, event filtering, test fixtures, integration tests, and documentation with the breaking changes introduced in `eventsalsa/store` v0.1.0.

## Changes Made

### 1. Dependency Upgrade
- [`go.mod`](go.mod) & [`go.sum`](go.sum): Updated `github.com/eventsalsa/store` requirement from `v0.0.3` to `v0.1.0`.

### 2. Core Worker Engine
- [`worker.go`](worker.go):
  - Renamed `consumerAggregateTypes` to `consumerStreamTypes`.
  - Updated consumer interface invocation to `scopedConsumer.StreamTypes()`.
  - Updated `filterHandledEvents` parameter to `streamTypes []string` and event filtering logic to check `rows[i].StreamType`.
  - Updated `scanEventsForConsumerBatch` to call `consumerStreamTypes(registeredConsumer)`.

### 3. Unit & Integration Tests
- [`worker_test.go`](worker_test.go):
  - Updated `scopedRecordingConsumer` struct field to `streamTypes []string` and method to `StreamTypes() []string`.
  - Updated `makeEvent` helper to assign `StreamType`, `StreamID`, and `StreamVersion`.
  - Updated assertion verifying `handled[0].StreamType`.
- [`integration_test/helpers_test.go`](integration_test/helpers_test.go):
  - Updated `testEventBatch`, `testConsumer`, and `testConsumerEventRow` to use `StreamType`/`StreamID`/`streamTypes`.
  - Updated test database setup to drop and truncate `stream_heads` instead of `aggregate_heads`.
  - Updated `test_consumer_events` read model table columns to `stream_type` and `stream_id`.
  - Updated `storemigrations.Config` to configure `StreamHeadsTable: "stream_heads"`.
  - Updated event generation in `appendTestEvents` and `beginControlledAppend` to populate `StreamType` and `StreamID`.
- [`integration_test/scaling_test.go`](integration_test/scaling_test.go):
  - Updated `runProcessingStep` signature and parameter name from `aggregateType` to `streamType`.
- [`integration_test/worker_test.go`](integration_test/worker_test.go):
  - Updated `testEventBatch` struct literals to use `StreamType:` instead of `AggregateType:`.

### 4. Documentation
- [`README.md`](README.md):
  - Updated projection examples and `ScopedConsumer` interface documentation to show `StreamTypes() []string`.

## Verification Results

### Automated Tests
All checks were executed via `rtk` per repository conventions:

- **Formatting**: `rtk make fmt` — Clean
- **Linter**: `rtk make lint` — 0 issues
- **Unit Tests**: `rtk make test-unit` — All unit tests passed with race detector enabled
- **Integration Tests**: `rtk make test-integration` — All PostgreSQL integration tests passed with testcontainers-go
- **Full Verification Suite**: `rtk make check` — 100% passed
