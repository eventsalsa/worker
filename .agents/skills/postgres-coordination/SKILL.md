---
name: PostgreSQL Coordination & Transactional Integrity
description: Development rules for database operations, ensuring transaction safety, PgBouncer compatibility, and deadlock avoidance.
---

# PostgreSQL Coordination & Transactional Integrity

Use this skill when modifying database queries, updating transaction boundaries, adding table schemas, or changing locking and retry logic.

## 1. Lock Acquisition Order (Deadlock Avoidance)
To prevent deadlocks between concurrent consumer nodes or leader nodes, always acquire locks on coordination tables in a strict, deterministic order:
1. **Consumer Assignment** — Lock the ownership row in the assignments table first (e.g., via `ensureConsumerOwnership`).
2. **Consumer Checkpoint** — Lock/select the checkpoint row next (e.g., via `GetCheckpointForUpdate`).
3. **Other Tables** — Modify or select data in other tables (e.g., event logs, gap skips).

*Never* acquire a checkpoint lock before validating or holding the assignment ownership lock.

## 2. Exactly-Once / Transaction Binding
All operations executed inside a consumer's event processing loop must be bound to the active transaction:
* Use the `*sql.Tx` object passed as `tx`. Never execute queries directly against the global `*sql.DB` connection pool from inside processing handlers.
* Ensure errors occurring within the transaction context are bubble-up wrapped so the loop coordinator can trigger a rollback.

## 3. Connection Pooling & PgBouncer Safety
When writing new queries or implementing state features:
* **Avoid Session-Level State:** Do not rely on session-level advisory locks (`pg_try_advisory_lock`), `SET` local commands, or `LISTEN/NOTIFY` on general pool connections. If these are absolutely required (like for leader election or event dispatchers), they *must* run on a dedicated connection retrieved via `db.Conn(ctx)` and managed independently of the pool.
* **Driver-Agnostic Errors:** Always use driver-agnostic interfaces to extract SQLSTATE codes (e.g., checking for code `40001` serialization conflicts across both `lib/pq` and `pgx/v5`). Do not cast directly to `*pq.Error` without handling alternatives.

## 4. Query Parameterization
* Always use parameterized queries (`$1`, `$2`, etc.) to prevent SQL injection.
* Avoid raw string interpolation for query criteria. Table names may be interpolated only after passing strict validation (e.g., via `resolveTableName`).
