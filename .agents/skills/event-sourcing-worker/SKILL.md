---
name: Event Sourcing & Marten-Style Projector Coordination
description: Architecture rules for asynchronous projection daemons, checkpointing, projection assignments, and rebalancing.
---

# Event Sourcing & Marten-Style Projector Coordination

This skill provides architectural guidelines for Marten-style projector coordination, projection processing, and rebalancing.

## Core Rules

1. **Marten-Style Coordination:**
   - All projector state (instances, assignments, checkpoints) must be stored in the PostgreSQL database. No external orchestrators (like Redis, ZooKeeper, or Consul) are allowed.
   - The projector instances table acts as the registry of active instances. Instances register themselves and update their heartbeat periodically.

2. **Projection Rebalancing:**
   - Only the elected leader instance performs projection assignment rebalancing.
   - The rebalancing algorithm must be deterministic and balance the set of active projections evenly across live instances.
   - Rebalancing must be transactional, updating assignments in a single transaction to prevent duplicate assignments or dropped projections.

3. **Progress Tracking & Checkpoints:**
   - Tracks positions using checkpoints. A checkpoint represents the last successfully processed event position.
   - Skip gaps in positions safely. If a gap is detected and determined to be stale, skip it and audit the skip to prevent locking.
