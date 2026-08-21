package projector

import (
	"context"
	"testing"

	"github.com/eventsalsa/store"
	"github.com/google/uuid"
)

func TestFilterStreamTypes(t *testing.T) {
	mockProj := &recordingProjection{name: "test-proj"}
	filtered := FilterStreamTypes(mockProj, "order", "invoice")

	if filtered.Name() != "test-proj" {
		t.Fatalf("filtered.Name() = %q, want %q", filtered.Name(), "test-proj")
	}

	ctx := context.Background()

	// Matching event stream type
	matchingEvent := store.PersistedEvent{
		EventID:    uuid.New(),
		StreamType: "order",
		EventType:  "OrderCreated",
	}
	if err := filtered.Handle(ctx, nil, matchingEvent); err != nil {
		t.Fatalf("Handle matching error = %v", err)
	}
	if len(mockProj.handled) != 1 {
		t.Fatalf("expected 1 handled event, got %d", len(mockProj.handled))
	}

	// Non-matching event stream type
	nonMatchingEvent := store.PersistedEvent{
		EventID:    uuid.New(),
		StreamType: "user",
		EventType:  "UserRegistered",
	}
	if err := filtered.Handle(ctx, nil, nonMatchingEvent); err != nil {
		t.Fatalf("Handle non-matching error = %v", err)
	}
	if len(mockProj.handled) != 1 {
		t.Fatalf("expected still 1 handled event, got %d", len(mockProj.handled))
	}
}

func TestFilterStreamTypesEmptyAllowsAll(t *testing.T) {
	mockProj := &recordingProjection{name: "test-proj"}
	filtered := FilterStreamTypes(mockProj)

	ctx := context.Background()
	event := store.PersistedEvent{
		EventID:    uuid.New(),
		StreamType: "anything",
	}
	if err := filtered.Handle(ctx, nil, event); err != nil {
		t.Fatalf("Handle error = %v", err)
	}
	if len(mockProj.handled) != 1 {
		t.Fatalf("expected 1 handled event, got %d", len(mockProj.handled))
	}
}

func TestFilterEventTypes(t *testing.T) {
	mockProj := &recordingProjection{name: "test-proj"}
	filtered := FilterEventTypes(mockProj, "OrderPlaced", "OrderCancelled")

	if filtered.Name() != "test-proj" {
		t.Fatalf("filtered.Name() = %q, want %q", filtered.Name(), "test-proj")
	}

	ctx := context.Background()

	// Matching event type
	matchingEvent := store.PersistedEvent{
		EventID:    uuid.New(),
		StreamType: "order",
		EventType:  "OrderPlaced",
	}
	if err := filtered.Handle(ctx, nil, matchingEvent); err != nil {
		t.Fatalf("Handle matching error = %v", err)
	}
	if len(mockProj.handled) != 1 {
		t.Fatalf("expected 1 handled event, got %d", len(mockProj.handled))
	}

	// Non-matching event type
	nonMatchingEvent := store.PersistedEvent{
		EventID:    uuid.New(),
		StreamType: "order",
		EventType:  "PaymentReceived",
	}
	if err := filtered.Handle(ctx, nil, nonMatchingEvent); err != nil {
		t.Fatalf("Handle non-matching error = %v", err)
	}
	if len(mockProj.handled) != 1 {
		t.Fatalf("expected still 1 handled event, got %d", len(mockProj.handled))
	}
}
