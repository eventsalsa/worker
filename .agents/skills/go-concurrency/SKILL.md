---
name: Go Concurrency & Graceful Shutdown
description: Best practices for implementing safe Go concurrency, context propagation, goroutine lifecycle, and graceful shutdown.
---

# Go Concurrency & Graceful Shutdown

This skill provides guidelines and patterns for writing concurrent Go code in the `eventsalsa/worker` module.

## Core Rules

1. **Goroutine Lifecycle & Ownership:**
   - Every goroutine spawned must have a clear owner, a defined lifetime, and a mechanism for clean exit.
   - Never start a goroutine without knowing how and when it will stop. Use `sync.WaitGroup` to coordinate shutdown.
   - Do not leak goroutines. Ensure all spawned routines exit before the parent process or struct shuts down.

2. **Context Propagation:**
   - Always pass `context.Context` as the first parameter to functions performing I/O, database access, or blocking operations.
   - Respect context cancellation. Check `ctx.Err() != nil` or select on `<-ctx.Done()` in long-running loops.

3. **Graceful Shutdown:**
   - Structure loops to monitor context cancellation alongside tickers and channels:
     ```go
     for {
         select {
         case <-ctx.Done():
             return
         case <-ticker.C:
             // Do work
         }
     }
     ```
   - When tearing down, wait for active workers to complete. Use `defer` blocks to clean up resources, close connections, and close channels.

4. **Synchronization Primitives:**
   - Protect shared mutable state with `sync.Mutex` or `sync.RWMutex`.
   - Keep critical sections as small as possible to prevent bottlenecks and deadlocks. Do not perform I/O or database calls inside locked blocks.
