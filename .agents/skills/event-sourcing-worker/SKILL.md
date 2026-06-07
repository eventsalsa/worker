---
name: Event Sourcing & Marten-Style Worker Coordination
description: Architecture rules for asynchronous projection daemons, checkpointing, consumer assignments, and rebalancing.
---

# Event Sourcing & Marten-Style Worker Coordination

This skill provides architectural guidelines for Marten-style worker coordination, projection processing, and rebalancing.

## Core Rules

1. **Marten-Style Coordination:**
   - All worker state (nodes, assignments, checkpoints) must be stored in the PostgreSQL database. No external orchestrators (like Redis, ZooKeeper, or Consul) are allowed.
   - The worker nodes table acts as the registry of active nodes. Nodes register themselves and update their heartbeat periodically.

2. **Consumer Rebalancing:**
   - Only the elected leader node performs consumer assignment rebalancing.
   - The rebalancing algorithm must be deterministic and balance the set of active consumers evenly across live nodes.
   - Rebalancing must be transactional, updating assignments in a single transaction to prevent duplicate assignments or dropped consumers.

3. **Progress Tracking & Checkpoints:**
   - Tracks positions using checkpoints. A checkpoint represents the last successfully processed event position.
   - Skip gaps in positions safely. If a gap is detected and determined to be stale, skip it and audit the skip to prevent locking.
