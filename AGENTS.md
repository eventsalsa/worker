# Agent Configuration

This repository is configured for use with agentic coding assistants. All development operations, task planning, and testing should follow the conventions and verification rules defined below.

## Git Conventions

All contributors (including AI agents) must strictly follow these git practices:

1. **Branching Strategy:**
   - **Never work directly on the `main` branch.**
   - When starting a new task, if you are currently on the `main` branch, you must open a new conventional branch.
   - Branch names must follow conventional patterns (e.g., `feat/`, `fix/`, `chore/`, `refactor/`, `test/`).

2. **Commit Messages:**
   - All commits must follow the **Conventional Commits** specification.
   - Format: `<type>(<scope>): <summary>` or `<type>: <summary>`.
   - **Every commit must include a descriptive body** after a blank line. Do not create bodyless commits. The body should explain the rationale, design decisions, and context for the change.

   *Example commit message:*
   ```text
   feat(leader): migrate to lease-based leader election

   Migrates the leader election coordination from session-level advisory locks
   to a lease-based table heartbeat mechanism. This ensures the daemon can run
   safely behind PgBouncer in transaction pooling mode.
   ```

## Verification Checks

Anytime code is touched, the agent is required to run all checks prior to committing or finalizing the task.

- **Required Command (Must use `rtk` prefix):**
  ```bash
  rtk make check
  ```
- **Rule Reference:** Always prefix shell commands with `rtk` as defined in the [RTK rules](.agents/rules/antigravity-rtk-rules.md).
- **Exceptions:** The only exception to running these checks is when the changes are made *exclusively* to markdown (`.md`) files.
