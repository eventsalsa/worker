// Package dispatcher provides wakeup signal dispatchers for consumer goroutines.
//
// A dispatcher is a lightweight optimization that detects when the event store has
// advanced and nudges worker consumers to poll immediately instead of waiting for
// their adaptive polling backoff to expire.
//
// Two strategies are provided:
//
//   - PollDispatcher periodically queries the latest global position. This is the
//     default strategy because it works with standard PostgreSQL deployments,
//     PgBouncer, and PostgreSQL proxies.
//   - NotifyDispatcher uses PostgreSQL LISTEN/NOTIFY for lower-latency wakeups and
//     performs a slower reconciliation poll as a fallback when notifications are
//     unavailable.
//
// NotifyDispatcher requires a dedicated PostgreSQL connection for LISTEN/NOTIFY.
// It does not work with PgBouncer in transaction pooling mode and is generally
// incompatible with proxies that do not preserve session state.
package dispatcher
