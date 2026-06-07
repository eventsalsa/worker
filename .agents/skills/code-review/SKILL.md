---
name: Correctness & Concurrency Code Review
description: Protocol for reviewing code changes to ensure transactional safety, concurrency correctness, and API design.
---

# Correctness & Concurrency Code Review

This skill covers how to review pull requests and code modifications in the worker project.

## Review Protocol

1. **Safety First:**
   - Scan for concurrency issues: unprotected shared maps/structs, potential goroutine leaks, and unhandled channel blockages.
   - Verify transaction isolation levels and error wrapping.

2. **API & Go Idioms:**
   - Check if public APIs are consistent with `eventsalsa/store`.
   - Ensure functional options are used for configuration.
   - Verify all public symbols are documented.

3. **Verify Integrity & Tests:**
   - Ensure every feature has corresponding unit/integration tests.
   - Verify that test cases cover failure recovery (rollback, retry, worker death).
