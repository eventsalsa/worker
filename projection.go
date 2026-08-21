package projector

import (
	"context"

	"github.com/eventsalsa/store"
	"github.com/jackc/pgx/v5"
)

// Projection defines the interface for event projection handlers.
// Projections are storage-agnostic and can write to any destination
// (SQL databases, NoSQL stores, search engines, in-memory caches, etc.).
type Projection interface {
	// Name returns the unique name of this projection.
	// This name is used for checkpoint tracking and distributed assignment.
	Name() string

	// Handle processes a single event.
	// Return an error to stop projection processing and trigger retry/backoff.
	//
	// The tx parameter is the daemon's transaction used for checkpoint management.
	// SQL projections can use this transaction to ensure atomic updates of both
	// the read model and the checkpoint. This eliminates inconsistencies where
	// a projection succeeds but the checkpoint update fails (or vice versa).
	// It does not by itself guarantee that checkpointing to the highest seen
	// global position is safe; processors still need a gap-aware runtime/frontier
	// before advancing checkpoints under concurrent writers.
	//
	// The transaction will be committed by the daemon after Handle returns successfully.
	// Projections should NEVER call Commit() or Rollback() on the provided transaction.
	//
	// For non-SQL projections (Elasticsearch, Redis, message brokers), the tx parameter
	// should be ignored and projections should manage their own connections as before.
	//
	// Event is passed by value to enforce immutability (events are value objects).
	// Large data (Payload, Metadata byte slices) share references to their backing arrays,
	// so the actual payload/metadata data is not deep-copied.
	//
	//nolint:gocritic // hugeParam: Intentionally pass by value to enforce immutability
	Handle(ctx context.Context, tx pgx.Tx, event store.PersistedEvent) error
}

type streamTypeFilter struct {
	Projection
	allowed map[string]struct{}
}

// FilterStreamTypes wraps a Projection so that it only processes events
// belonging to the specified stream types. Events from other stream types
// are skipped without error, allowing checkpoints to advance safely.
func FilterStreamTypes(p Projection, streamTypes ...string) Projection {
	allowed := make(map[string]struct{}, len(streamTypes))
	for _, st := range streamTypes {
		allowed[st] = struct{}{}
	}

	return &streamTypeFilter{
		Projection: p,
		allowed:    allowed,
	}
}

// Handle processes the event if its StreamType matches the allowed stream types.
//
//nolint:gocritic // hugeParam: event matches the store.PersistedEvent value contract
func (f *streamTypeFilter) Handle(ctx context.Context, tx pgx.Tx, event store.PersistedEvent) error {
	if len(f.allowed) > 0 {
		if _, ok := f.allowed[event.StreamType]; !ok {
			return nil
		}
	}

	return f.Projection.Handle(ctx, tx, event)
}

// StreamTypes returns the slice of allowed stream types for introspection.
func (f *streamTypeFilter) StreamTypes() []string {
	types := make([]string, 0, len(f.allowed))
	for st := range f.allowed {
		types = append(types, st)
	}
	return types
}

type eventTypeFilter struct {
	Projection
	allowed map[string]struct{}
}

// FilterEventTypes wraps a Projection so that it only processes events
// matching the specified event types. Events with other event types
// are skipped without error, allowing checkpoints to advance safely.
func FilterEventTypes(p Projection, eventTypes ...string) Projection {
	allowed := make(map[string]struct{}, len(eventTypes))
	for _, et := range eventTypes {
		allowed[et] = struct{}{}
	}

	return &eventTypeFilter{
		Projection: p,
		allowed:    allowed,
	}
}

// Handle processes the event if its EventType matches the allowed event types.
//
//nolint:gocritic // hugeParam: event matches the store.PersistedEvent value contract
func (f *eventTypeFilter) Handle(ctx context.Context, tx pgx.Tx, event store.PersistedEvent) error {
	if len(f.allowed) > 0 {
		if _, ok := f.allowed[event.EventType]; !ok {
			return nil
		}
	}

	return f.Projection.Handle(ctx, tx, event)
}

// EventTypes returns the slice of allowed event types for introspection.
func (f *eventTypeFilter) EventTypes() []string {
	types := make([]string, 0, len(f.allowed))
	for et := range f.allowed {
		types = append(types, et)
	}
	return types
}
