package postgres

import (
	"fmt"
	"slices"
	"testing"

	"github.com/google/uuid"
)

func TestComputeAssignmentsEvenDistribution(t *testing.T) {
	instanceIDs := []uuid.UUID{
		uuid.MustParse("00000000-0000-0000-0000-000000000001"),
		uuid.MustParse("00000000-0000-0000-0000-000000000002"),
	}
	projectionNames := []string{
		"projection-10",
		"projection-03",
		"projection-06",
		"projection-04",
		"projection-01",
		"projection-09",
		"projection-05",
		"projection-07",
		"projection-02",
		"projection-08",
	}

	assignments := ComputeAssignments(projectionNames, instanceIDs)

	if len(assignments) != len(projectionNames) {
		t.Fatalf("expected %d assignments, got %d", len(projectionNames), len(assignments))
	}

	counts := countAssignments(assignments)
	if counts[instanceIDs[0]] != 5 || counts[instanceIDs[1]] != 5 {
		t.Fatalf("expected 5 assignments per instance, got %v", counts)
	}
}

func TestComputeAssignmentsUnevenDistribution(t *testing.T) {
	instanceIDs := []uuid.UUID{
		uuid.MustParse("00000000-0000-0000-0000-000000000001"),
		uuid.MustParse("00000000-0000-0000-0000-000000000002"),
		uuid.MustParse("00000000-0000-0000-0000-000000000003"),
	}
	projectionNames := []string{
		"projection-g",
		"projection-a",
		"projection-e",
		"projection-c",
		"projection-b",
		"projection-d",
		"projection-f",
	}

	assignments := ComputeAssignments(projectionNames, instanceIDs)
	counts := countAssignments(assignments)

	got := []int{counts[instanceIDs[0]], counts[instanceIDs[1]], counts[instanceIDs[2]]}
	want := []int{3, 2, 2}
	if !slices.Equal(got, want) {
		t.Fatalf("expected uneven distribution %v, got %v", want, got)
	}
}

func TestComputeAssignmentsSingleInstance(t *testing.T) {
	instanceID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	projectionNames := []string{"alpha", "beta", "gamma"}

	assignments := ComputeAssignments(projectionNames, []uuid.UUID{instanceID})

	for _, projectionName := range projectionNames {
		if assignments[projectionName] != instanceID {
			t.Fatalf("expected projection %s to be assigned to %s, got %s", projectionName, instanceID, assignments[projectionName])
		}
	}
}

func TestComputeAssignmentsNoInstances(t *testing.T) {
	assignments := ComputeAssignments([]string{"alpha", "beta"}, nil)
	if len(assignments) != 0 {
		t.Fatalf("expected no assignments, got %v", assignments)
	}
}

func TestComputeAssignmentsNoProjections(t *testing.T) {
	assignments := ComputeAssignments(nil, []uuid.UUID{uuid.New()})
	if len(assignments) != 0 {
		t.Fatalf("expected no assignments, got %v", assignments)
	}
}

func TestComputeAssignmentsDeterministic(t *testing.T) {
	instanceIDs := []uuid.UUID{
		uuid.MustParse("00000000-0000-0000-0000-000000000010"),
		uuid.MustParse("00000000-0000-0000-0000-000000000001"),
		uuid.MustParse("00000000-0000-0000-0000-000000000007"),
	}
	projectionNames := []string{"gamma", "alpha", "delta", "beta"}

	first := ComputeAssignments(projectionNames, instanceIDs)
	second := ComputeAssignments(projectionNames, instanceIDs)

	if len(first) != len(second) {
		t.Fatalf("expected same assignment length, got %d and %d", len(first), len(second))
	}

	for projectionName, firstInstanceID := range first {
		if second[projectionName] != firstInstanceID {
			t.Fatalf("expected deterministic assignment for %s, got %s then %s", projectionName, firstInstanceID, second[projectionName])
		}
	}
}

func TestNeedsRebalance(t *testing.T) {
	instanceA := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	instanceB := uuid.MustParse("00000000-0000-0000-0000-000000000002")

	t.Run("returns false when assignments already match ideal state", func(t *testing.T) {
		current := []ProjectionAssignment{
			{ProjectionName: "alpha", InstanceID: instanceA, Assigned: true},
			{ProjectionName: "beta", InstanceID: instanceB, Assigned: true},
			{ProjectionName: "delta", InstanceID: instanceA, Assigned: true},
			{ProjectionName: "gamma", InstanceID: instanceB, Assigned: true},
		}

		if NeedsRebalance(current, []uuid.UUID{instanceA, instanceB}) {
			t.Fatal("expected rebalance to be unnecessary")
		}
	})

	t.Run("returns true when instance topology changes", func(t *testing.T) {
		current := []ProjectionAssignment{
			{ProjectionName: "alpha", InstanceID: instanceA, Assigned: true},
			{ProjectionName: "beta", InstanceID: instanceA, Assigned: true},
			{ProjectionName: "gamma", InstanceID: instanceA, Assigned: true},
			{ProjectionName: "delta", InstanceID: instanceA, Assigned: true},
		}

		if !NeedsRebalance(current, []uuid.UUID{instanceA, instanceB}) {
			t.Fatal("expected rebalance to be required after adding a live instance")
		}
	})

	t.Run("returns true when no instances are live but assignments remain", func(t *testing.T) {
		current := []ProjectionAssignment{
			{ProjectionName: "alpha", InstanceID: instanceA, Assigned: true},
		}

		if !NeedsRebalance(current, nil) {
			t.Fatal("expected rebalance to be required when assignments should become unassigned")
		}
	})

	t.Run("returns false when no instances are live and projections are already unassigned", func(t *testing.T) {
		current := []ProjectionAssignment{
			{ProjectionName: "alpha"},
			{ProjectionName: "beta"},
		}

		if NeedsRebalance(current, nil) {
			t.Fatal("expected rebalance to be unnecessary when projections are already unassigned")
		}
	})
}

func TestComputeAssignmentsDuplicateInstancesRemainDeterministic(t *testing.T) {
	instanceA := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	instanceB := uuid.MustParse("00000000-0000-0000-0000-000000000002")
	projectionNames := []string{"gamma", "alpha", "zeta", "beta", "epsilon", "delta"}
	instanceIDs := []uuid.UUID{instanceB, instanceA, instanceA}

	first := ComputeAssignments(projectionNames, instanceIDs)
	second := ComputeAssignments([]string{"delta", "epsilon", "beta", "zeta", "alpha", "gamma"}, []uuid.UUID{instanceA, instanceB, instanceA})

	if len(first) != len(projectionNames) {
		t.Fatalf("expected %d assignments, got %d", len(projectionNames), len(first))
	}

	for projectionName, assignedInstance := range first {
		if second[projectionName] != assignedInstance {
			t.Fatalf("assignment for %s = %s then %s, want deterministic result", projectionName, assignedInstance, second[projectionName])
		}
	}

	counts := countAssignments(first)
	if counts[instanceA] != 4 || counts[instanceB] != 2 {
		t.Fatalf("assignment counts = %v, want instanceA=4 instanceB=2", counts)
	}
}

func TestComputeAssignmentsLargeProjectionSetRemainsBalanced(t *testing.T) {
	instanceIDs := []uuid.UUID{
		uuid.MustParse("00000000-0000-0000-0000-000000000001"),
		uuid.MustParse("00000000-0000-0000-0000-000000000002"),
		uuid.MustParse("00000000-0000-0000-0000-000000000003"),
		uuid.MustParse("00000000-0000-0000-0000-000000000004"),
	}

	projectionNames := make([]string, 0, 101)
	for i := 100; i >= 0; i-- {
		projectionNames = append(projectionNames, fmt.Sprintf("projection-%03d", i))
	}

	assignments := ComputeAssignments(projectionNames, instanceIDs)
	if len(assignments) != len(projectionNames) {
		t.Fatalf("expected %d assignments, got %d", len(projectionNames), len(assignments))
	}

	counts := countAssignments(assignments)
	got := []int{counts[instanceIDs[0]], counts[instanceIDs[1]], counts[instanceIDs[2]], counts[instanceIDs[3]]}
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
	for _, instanceID := range assignments {
		counts[instanceID]++
	}

	return counts
}
