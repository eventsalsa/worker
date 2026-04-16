package postgres

import (
	"sort"

	"github.com/google/uuid"
)

// ComputeAssignments distributes consumers evenly across workers.
// It sorts consumers alphabetically and workers by UUID string before assigning in round-robin order.
func ComputeAssignments(consumerNames []string, workerIDs []uuid.UUID) map[string]uuid.UUID {
	if len(consumerNames) == 0 || len(workerIDs) == 0 {
		return map[string]uuid.UUID{}
	}

	sortedConsumers := append([]string(nil), consumerNames...)
	sort.Strings(sortedConsumers)

	sortedWorkers := append([]uuid.UUID(nil), workerIDs...)
	sort.Slice(sortedWorkers, func(i, j int) bool {
		return sortedWorkers[i].String() < sortedWorkers[j].String()
	})

	assignments := make(map[string]uuid.UUID, len(sortedConsumers))
	for index, consumerName := range sortedConsumers {
		assignments[consumerName] = sortedWorkers[index%len(sortedWorkers)]
	}

	return assignments
}

// NeedsRebalance checks if the current assignments differ from the ideal balanced distribution.
func NeedsRebalance(current []ConsumerAssignment, liveWorkers []uuid.UUID) bool {
	if len(current) == 0 {
		return false
	}

	consumerNames := make([]string, 0, len(current))
	for _, assignment := range current {
		consumerNames = append(consumerNames, assignment.ConsumerName)
	}

	ideal := ComputeAssignments(consumerNames, liveWorkers)

	for _, assignment := range current {
		idealWorkerID, shouldBeAssigned := ideal[assignment.ConsumerName]
		if assignment.Assigned != shouldBeAssigned {
			return true
		}
		if assignment.Assigned && assignment.WorkerID != idealWorkerID {
			return true
		}
	}

	return false
}
