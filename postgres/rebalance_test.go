package postgres

import (
	"fmt"
	"slices"
	"testing"

	"github.com/google/uuid"
)

func TestComputeAssignmentsEvenDistribution(t *testing.T) {
	workerIDs := []uuid.UUID{
		uuid.MustParse("00000000-0000-0000-0000-000000000001"),
		uuid.MustParse("00000000-0000-0000-0000-000000000002"),
	}
	consumerNames := []string{
		"consumer-10",
		"consumer-03",
		"consumer-06",
		"consumer-04",
		"consumer-01",
		"consumer-09",
		"consumer-05",
		"consumer-07",
		"consumer-02",
		"consumer-08",
	}

	assignments := ComputeAssignments(consumerNames, workerIDs)

	if len(assignments) != len(consumerNames) {
		t.Fatalf("expected %d assignments, got %d", len(consumerNames), len(assignments))
	}

	counts := countAssignments(assignments)
	if counts[workerIDs[0]] != 5 || counts[workerIDs[1]] != 5 {
		t.Fatalf("expected 5 assignments per worker, got %v", counts)
	}
}

func TestComputeAssignmentsUnevenDistribution(t *testing.T) {
	workerIDs := []uuid.UUID{
		uuid.MustParse("00000000-0000-0000-0000-000000000001"),
		uuid.MustParse("00000000-0000-0000-0000-000000000002"),
		uuid.MustParse("00000000-0000-0000-0000-000000000003"),
	}
	consumerNames := []string{
		"consumer-g",
		"consumer-a",
		"consumer-e",
		"consumer-c",
		"consumer-b",
		"consumer-d",
		"consumer-f",
	}

	assignments := ComputeAssignments(consumerNames, workerIDs)
	counts := countAssignments(assignments)

	got := []int{counts[workerIDs[0]], counts[workerIDs[1]], counts[workerIDs[2]]}
	want := []int{3, 2, 2}
	if !slices.Equal(got, want) {
		t.Fatalf("expected uneven distribution %v, got %v", want, got)
	}
}

func TestComputeAssignmentsSingleWorker(t *testing.T) {
	workerID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	consumerNames := []string{"alpha", "beta", "gamma"}

	assignments := ComputeAssignments(consumerNames, []uuid.UUID{workerID})

	for _, consumerName := range consumerNames {
		if assignments[consumerName] != workerID {
			t.Fatalf("expected consumer %s to be assigned to %s, got %s", consumerName, workerID, assignments[consumerName])
		}
	}
}

func TestComputeAssignmentsNoWorkers(t *testing.T) {
	assignments := ComputeAssignments([]string{"alpha", "beta"}, nil)
	if len(assignments) != 0 {
		t.Fatalf("expected no assignments, got %v", assignments)
	}
}

func TestComputeAssignmentsNoConsumers(t *testing.T) {
	assignments := ComputeAssignments(nil, []uuid.UUID{uuid.New()})
	if len(assignments) != 0 {
		t.Fatalf("expected no assignments, got %v", assignments)
	}
}

func TestComputeAssignmentsDeterministic(t *testing.T) {
	workerIDs := []uuid.UUID{
		uuid.MustParse("00000000-0000-0000-0000-000000000010"),
		uuid.MustParse("00000000-0000-0000-0000-000000000001"),
		uuid.MustParse("00000000-0000-0000-0000-000000000007"),
	}
	consumerNames := []string{"gamma", "alpha", "delta", "beta"}

	first := ComputeAssignments(consumerNames, workerIDs)
	second := ComputeAssignments(consumerNames, workerIDs)

	if len(first) != len(second) {
		t.Fatalf("expected same assignment length, got %d and %d", len(first), len(second))
	}

	for consumerName, firstWorkerID := range first {
		if second[consumerName] != firstWorkerID {
			t.Fatalf("expected deterministic assignment for %s, got %s then %s", consumerName, firstWorkerID, second[consumerName])
		}
	}
}

func TestNeedsRebalance(t *testing.T) {
	workerA := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	workerB := uuid.MustParse("00000000-0000-0000-0000-000000000002")

	t.Run("returns false when assignments already match ideal state", func(t *testing.T) {
		current := []ConsumerAssignment{
			{ConsumerName: "alpha", WorkerID: workerA, Assigned: true},
			{ConsumerName: "beta", WorkerID: workerB, Assigned: true},
			{ConsumerName: "delta", WorkerID: workerA, Assigned: true},
			{ConsumerName: "gamma", WorkerID: workerB, Assigned: true},
		}

		if NeedsRebalance(current, []uuid.UUID{workerA, workerB}) {
			t.Fatal("expected rebalance to be unnecessary")
		}
	})

	t.Run("returns true when worker topology changes", func(t *testing.T) {
		current := []ConsumerAssignment{
			{ConsumerName: "alpha", WorkerID: workerA, Assigned: true},
			{ConsumerName: "beta", WorkerID: workerA, Assigned: true},
			{ConsumerName: "gamma", WorkerID: workerA, Assigned: true},
			{ConsumerName: "delta", WorkerID: workerA, Assigned: true},
		}

		if !NeedsRebalance(current, []uuid.UUID{workerA, workerB}) {
			t.Fatal("expected rebalance to be required after adding a live worker")
		}
	})

	t.Run("returns true when no workers are live but assignments remain", func(t *testing.T) {
		current := []ConsumerAssignment{
			{ConsumerName: "alpha", WorkerID: workerA, Assigned: true},
		}

		if !NeedsRebalance(current, nil) {
			t.Fatal("expected rebalance to be required when assignments should become unassigned")
		}
	})

	t.Run("returns false when no workers are live and consumers are already unassigned", func(t *testing.T) {
		current := []ConsumerAssignment{
			{ConsumerName: "alpha"},
			{ConsumerName: "beta"},
		}

		if NeedsRebalance(current, nil) {
			t.Fatal("expected rebalance to be unnecessary when consumers are already unassigned")
		}
	})
}

func TestComputeAssignmentsDuplicateWorkersRemainDeterministic(t *testing.T) {
	workerA := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	workerB := uuid.MustParse("00000000-0000-0000-0000-000000000002")
	consumerNames := []string{"gamma", "alpha", "zeta", "beta", "epsilon", "delta"}
	workerIDs := []uuid.UUID{workerB, workerA, workerA}

	first := ComputeAssignments(consumerNames, workerIDs)
	second := ComputeAssignments([]string{"delta", "epsilon", "beta", "zeta", "alpha", "gamma"}, []uuid.UUID{workerA, workerB, workerA})

	if len(first) != len(consumerNames) {
		t.Fatalf("expected %d assignments, got %d", len(consumerNames), len(first))
	}

	for consumerName, assignedWorker := range first {
		if second[consumerName] != assignedWorker {
			t.Fatalf("assignment for %s = %s then %s, want deterministic result", consumerName, assignedWorker, second[consumerName])
		}
	}

	counts := countAssignments(first)
	if counts[workerA] != 4 || counts[workerB] != 2 {
		t.Fatalf("assignment counts = %v, want workerA=4 workerB=2", counts)
	}
}

func TestComputeAssignmentsLargeConsumerSetRemainsBalanced(t *testing.T) {
	workerIDs := []uuid.UUID{
		uuid.MustParse("00000000-0000-0000-0000-000000000001"),
		uuid.MustParse("00000000-0000-0000-0000-000000000002"),
		uuid.MustParse("00000000-0000-0000-0000-000000000003"),
		uuid.MustParse("00000000-0000-0000-0000-000000000004"),
	}

	consumerNames := make([]string, 0, 101)
	for i := 100; i >= 0; i-- {
		consumerNames = append(consumerNames, fmt.Sprintf("consumer-%03d", i))
	}

	assignments := ComputeAssignments(consumerNames, workerIDs)
	if len(assignments) != len(consumerNames) {
		t.Fatalf("expected %d assignments, got %d", len(consumerNames), len(assignments))
	}

	counts := countAssignments(assignments)
	got := []int{counts[workerIDs[0]], counts[workerIDs[1]], counts[workerIDs[2]], counts[workerIDs[3]]}
	want := []int{26, 25, 25, 25}
	if !slices.Equal(got, want) {
		t.Fatalf("assignment counts = %v, want %v", got, want)
	}

	minCount, maxCount := got[0], got[0]
	for _, count := range got[1:] {
		if count < minCount {
			minCount = count
		}
		if count > maxCount {
			maxCount = count
		}
	}
	if maxCount-minCount > 1 {
		t.Fatalf("distribution is not balanced: counts=%v", got)
	}
}

func countAssignments(assignments map[string]uuid.UUID) map[uuid.UUID]int {
	counts := make(map[uuid.UUID]int, len(assignments))
	for _, workerID := range assignments {
		counts[workerID]++
	}

	return counts
}
