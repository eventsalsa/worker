package postgres

import (
	"sort"

	"github.com/google/uuid"
)

// ComputeAssignments distributes projections evenly across instances.
// It sorts projections alphabetically and instances by UUID string before assigning in round-robin order.
func ComputeAssignments(projectionNames []string, instanceIDs []uuid.UUID) map[string]uuid.UUID {
	if len(projectionNames) == 0 || len(instanceIDs) == 0 {
		return map[string]uuid.UUID{}
	}

	sortedProjections := append([]string(nil), projectionNames...)
	sort.Strings(sortedProjections)

	sortedInstances := append([]uuid.UUID(nil), instanceIDs...)
	sort.Slice(sortedInstances, func(i, j int) bool {
		return sortedInstances[i].String() < sortedInstances[j].String()
	})

	assignments := make(map[string]uuid.UUID, len(sortedProjections))
	for index, projectionName := range sortedProjections {
		assignments[projectionName] = sortedInstances[index%len(sortedInstances)]
	}

	return assignments
}

// NeedsRebalance checks if the current assignments differ from the ideal balanced distribution.
func NeedsRebalance(current []ProjectionAssignment, liveInstances []uuid.UUID) bool {
	if len(current) == 0 {
		return false
	}

	projectionNames := make([]string, 0, len(current))
	for _, assignment := range current {
		projectionNames = append(projectionNames, assignment.ProjectionName)
	}

	ideal := ComputeAssignments(projectionNames, liveInstances)

	for _, assignment := range current {
		idealInstanceID, shouldBeAssigned := ideal[assignment.ProjectionName]
		if assignment.Assigned != shouldBeAssigned {
			return true
		}
		if assignment.Assigned && assignment.InstanceID != idealInstanceID {
			return true
		}
	}

	return false
}
