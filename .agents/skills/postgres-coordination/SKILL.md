---
name: PostgreSQL Coordination & Transactional Integrity
description: Development rules for database operations, ensuring transaction safety, PgBouncer compatibility, and deadlock avoidance.
---

# PostgreSQL Coordination & Transactional Integrity

Use this skill when modifying database queries, updating transaction boundaries, adding table schemas, or changing locking and retry logic.

## 1. Lock Acquisition Order (Deadlock Avoidance)
To prevent deadlocks between concurrent projector instances or leader instances, always acquire locks on coordination tables in a strict, deterministic order:
1. **Projection Assignment** — Lock the ownership row in the assignments table first (e.g., via `ensureProjectionOwnership`).
2. **Projection Checkpoint** — Lock/select the checkpoint row next (e.g., via `GetCheckpointForUpdate`).
3. **Other Tables** — Modify or select data in other tables (e.g., event logs, gap skips).

*Never* acquire a checkpoint lock before validating or holding the assignment ownership lock.

## 2. Exactly-Once / Transaction Binding
All operations executed inside a projection's event processing loop must be bound to the active transaction:
* Use the `pgx.Tx` object passed as `tx`. Never execute queries directly against the global `*pgxpool.Pool` connection pool from inside processing handlers.
* Ensure errors occurring within the transaction context are bubble-up wrapped so the loop coordinator can trigger a rollback.

## 3. Connection Pooling & PgBouncer Safety
When writing new queries or implementing state features:
* **Avoid Session-Level State on Pooled Connections:** Do not rely on session-level advisory locks (`pg_try_advisory_lock`), `SET` local commands, or `LISTEN/NOTIFY` on general pool connections. When these are used (like advisory leader election or notify dispatchers), they *must* run on a dedicated connection retrieved via `db.Acquire(ctx)` and managed independently of the pool.
* **Lease-Based Leader Strategy:** For deployments behind PgBouncer in transaction pooling mode, use `LeaderStrategyLease` which coordinates leadership via table heartbeats using short-lived transactions.
* **Error Handling:** Check for standard PostgreSQL error codes (e.g., code `40001` serialization conflicts via `pgconn.PgError`).

## 4. Query Parameterization
* Always use parameterized queries (`$1`, `$2`, etc.) to prevent SQL injection.
* Avoid raw string interpolation for query criteria. Table names may be interpolated only after passing strict validation (e.g., via `resolveTableName`).
