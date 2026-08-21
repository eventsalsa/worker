package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// ProjectionAssignment represents a row in the projection_assignments table.
type ProjectionAssignment struct {
	ProjectionName string
	InstanceID     uuid.UUID
	Assigned       bool
}

// EnsureProjectionsRegistered upserts projection names into the assignment table.
// New projections get NULL instance_id. Existing projections are not modified.
func EnsureProjectionsRegistered(ctx context.Context, db DB, table string, projectionNames []string) error {
	if len(projectionNames) == 0 {
		return nil
	}

	table = resolveTableName(table, DefaultProjectionAssignmentsTable)

	args := make([]any, 0, len(projectionNames))
	values := make([]string, 0, len(projectionNames))
	for index, projectionName := range projectionNames {
		args = append(args, projectionName)
		values = append(values, fmt.Sprintf("($%d, NULL, NOW(), NOW())", index+1))
	}

	//nolint:gosec // G201: table name comes from trusted configuration.
	query := fmt.Sprintf(`
		INSERT INTO %s (projection_name, instance_id, created_at, updated_at)
		VALUES %s
		ON CONFLICT (projection_name) DO NOTHING
	`, table, strings.Join(values, ", "))

	if _, err := db.Exec(ctx, query, args...); err != nil {
		return fmt.Errorf("ensure projections registered: %w", err)
	}

	return nil
}

// GetAssignments returns all projection assignments.
func GetAssignments(ctx context.Context, db DB, table string) ([]ProjectionAssignment, error) {
	table = resolveTableName(table, DefaultProjectionAssignmentsTable)

	//nolint:gosec // G201: table name comes from trusted configuration.
	query := fmt.Sprintf(`
		SELECT projection_name, instance_id
		FROM %s
		ORDER BY projection_name ASC
	`, table)

	rows, err := db.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("get assignments: %w", err)
	}
	defer rows.Close()

	assignments := make([]ProjectionAssignment, 0)
	for rows.Next() {
		var assignment ProjectionAssignment
		var instanceID *uuid.UUID

		if err := rows.Scan(&assignment.ProjectionName, &instanceID); err != nil {
			return nil, fmt.Errorf("scan assignment: %w", err)
		}

		if instanceID != nil {
			assignment.InstanceID = *instanceID
			assignment.Assigned = true
		}

		assignments = append(assignments, assignment)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate assignments: %w", err)
	}

	return assignments, nil
}

// SetAssignments atomically updates instance_id for the given projection-to-instance mapping.
// This should be called within a transaction by the leader during rebalancing.
func SetAssignments(ctx context.Context, tx pgx.Tx, table string, assignments map[string]uuid.UUID) error {
	if len(assignments) == 0 {
		return nil
	}

	table = resolveTableName(table, DefaultProjectionAssignmentsTable)

	//nolint:gosec // G201: table name comes from trusted configuration.
	query := fmt.Sprintf(`
		UPDATE %s
		SET instance_id = $1, updated_at = NOW()
		WHERE projection_name = $2
	`, table)

	for projectionName, instanceID := range assignments {
		if _, err := tx.Exec(ctx, query, instanceID, projectionName); err != nil {
			return fmt.Errorf("set assignment for projection %s: %w", projectionName, err)
		}
	}

	return nil
}
