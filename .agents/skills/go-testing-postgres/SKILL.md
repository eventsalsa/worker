---
name: Go Testing & Integration with PostgreSQL
description: Guidelines for unit and integration testing, testcontainers-go lifecycle, and concurrent/async assertions.
---

# Go Testing & Integration with PostgreSQL

This skill outlines how to write reliable unit and integration tests for the worker module.

## Core Rules

1. **Unit Testing:**
   - Test business logic and helpers in isolation using standard mock objects or interfaces.
   - Do not spin up a database for unit tests; use stub implementations for database connections and store interfaces.

2. **Integration Testing with PostgreSQL:**
   - Run integration tests against a real PostgreSQL instance using `testcontainers-go`.
   - Keep test containers isolated. Re-migrate or clean the schema between test cases to prevent test crosstalk.
   - Use Go build tags (e.g., `//go:build integration`) to separate integration tests from fast-running unit tests if needed.

3. **Testing Concurrent & Async Behaviors:**
   - Never use fixed `time.Sleep()` for asserting asynchronous events.
   - Use polling-with-timeout helpers to verify async states:
     ```go
     assert.Eventually(t, func() bool {
         // check condition
         return true
     }, 10*time.Second, 100*time.Millisecond)
     ```
   - Assert both expected outcomes (e.g., checkpoint advanced) and safety invariants (e.g., no duplicate event handling).
