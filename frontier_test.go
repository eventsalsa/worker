package worker

import (
	"testing"

	"github.com/eventsalsa/store"
)

func TestComputeSafeHarbor(t *testing.T) {
	makeEvent := func(position int64) store.PersistedEvent {
		return store.PersistedEvent{GlobalPosition: position}
	}

	t.Run("respects lag within the contiguous prefix after a stale gap", func(t *testing.T) {
		frontier, ok := computeSafeHarbor(2, []store.PersistedEvent{
			makeEvent(2),
			makeEvent(3),
			makeEvent(4),
			makeEvent(5),
			makeEvent(6),
		}, 1)
		if !ok {
			t.Fatal("computeSafeHarbor() ok = false, want true")
		}
		if frontier != 5 {
			t.Fatalf("computeSafeHarbor() frontier = %d, want 5", frontier)
		}
	})

	t.Run("caps lag to the visible window when later rows are sparse", func(t *testing.T) {
		frontier, ok := computeSafeHarbor(2, []store.PersistedEvent{
			makeEvent(2),
			makeEvent(3),
		}, 8)
		if !ok {
			t.Fatal("computeSafeHarbor() ok = false, want true")
		}
		if frontier != 2 {
			t.Fatalf("computeSafeHarbor() frontier = %d, want 2", frontier)
		}
	})

	t.Run("does not cross a fresh gap beyond the stale position", func(t *testing.T) {
		frontier, ok := computeSafeHarbor(2, []store.PersistedEvent{
			makeEvent(2),
			makeEvent(4),
			makeEvent(5),
			makeEvent(6),
		}, 1)
		if !ok {
			t.Fatal("computeSafeHarbor() ok = false, want true")
		}
		if frontier != 2 {
			t.Fatalf("computeSafeHarbor() frontier = %d, want 2", frontier)
		}
	})

	t.Run("falls back to the earliest visible row when lag exceeds a sparse window", func(t *testing.T) {
		frontier, ok := computeGapSkipTarget(0, []store.PersistedEvent{
			makeEvent(2),
			makeEvent(3),
		}, 8)
		if !ok {
			t.Fatal("computeGapSkipTarget() ok = false, want true")
		}
		if frontier != 2 {
			t.Fatalf("computeGapSkipTarget() frontier = %d, want 2", frontier)
		}
	})

	t.Run("skips consecutive leading gaps once visible rows start later", func(t *testing.T) {
		frontier, ok := computeGapSkipTarget(100, []store.PersistedEvent{
			makeEvent(102),
			makeEvent(103),
			makeEvent(104),
		}, 1)
		if !ok {
			t.Fatal("computeGapSkipTarget() ok = false, want true")
		}
		if frontier != 103 {
			t.Fatalf("computeGapSkipTarget() frontier = %d, want 103", frontier)
		}
	})

	t.Run("skips consecutive leading gaps even when the visible window is smaller than the lag", func(t *testing.T) {
		frontier, ok := computeGapSkipTarget(100, []store.PersistedEvent{
			makeEvent(103),
			makeEvent(104),
		}, 8)
		if !ok {
			t.Fatal("computeGapSkipTarget() ok = false, want true")
		}
		if frontier != 103 {
			t.Fatalf("computeGapSkipTarget() frontier = %d, want 103", frontier)
		}
	})
}
