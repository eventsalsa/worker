package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// ConsumerAssignment represents a row in the consumer_assignments table.
type ConsumerAssignment struct {
	ConsumerName string
	WorkerID     uuid.UUID
	Assigned     bool
}

// EnsureConsumersRegistered upserts consumer names into the assignment table.
// New consumers get NULL worker_id. Existing consumers are not modified.
func EnsureConsumersRegistered(ctx context.Context, db DB, table string, consumerNames []string) error {
	if len(consumerNames) == 0 {
		return nil
	}

	table = resolveTableName(table, DefaultConsumerAssignmentsTable)

	args := make([]any, 0, len(consumerNames))
	values := make([]string, 0, len(consumerNames))
	for index, consumerName := range consumerNames {
		args = append(args, consumerName)
		values = append(values, fmt.Sprintf("($%d, NULL, NOW(), NOW())", index+1))
	}

	//nolint:gosec // G201: table name comes from trusted configuration.
	query := fmt.Sprintf(`
		INSERT INTO %s (consumer_name, worker_id, created_at, updated_at)
		VALUES %s
		ON CONFLICT (consumer_name) DO NOTHING
	`, table, strings.Join(values, ", "))

	if _, err := db.Exec(ctx, query, args...); err != nil {
		return fmt.Errorf("ensure consumers registered: %w", err)
	}

	return nil
}

// GetAssignments returns all consumer assignments.
func GetAssignments(ctx context.Context, db DB, table string) ([]ConsumerAssignment, error) {
	table = resolveTableName(table, DefaultConsumerAssignmentsTable)

	//nolint:gosec // G201: table name comes from trusted configuration.
	query := fmt.Sprintf(`
		SELECT consumer_name, worker_id
		FROM %s
		ORDER BY consumer_name ASC
	`, table)

	rows, err := db.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("get assignments: %w", err)
	}
	defer rows.Close()

	assignments := make([]ConsumerAssignment, 0)
	for rows.Next() {
		var assignment ConsumerAssignment
		var workerID *uuid.UUID

		if err := rows.Scan(&assignment.ConsumerName, &workerID); err != nil {
			return nil, fmt.Errorf("scan assignment: %w", err)
		}

		if workerID != nil {
			assignment.WorkerID = *workerID
			assignment.Assigned = true
		}

		assignments = append(assignments, assignment)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate assignments: %w", err)
	}

	return assignments, nil
}

// SetAssignments atomically updates worker_id for the given consumer-to-worker mapping.
// This should be called within a transaction by the leader during rebalancing.
func SetAssignments(ctx context.Context, tx pgx.Tx, table string, assignments map[string]uuid.UUID) error {
	if len(assignments) == 0 {
		return nil
	}

	table = resolveTableName(table, DefaultConsumerAssignmentsTable)

	//nolint:gosec // G201: table name comes from trusted configuration.
	query := fmt.Sprintf(`
		UPDATE %s
		SET worker_id = $1, updated_at = NOW()
		WHERE consumer_name = $2
	`, table)

	for consumerName, workerID := range assignments {
		if _, err := tx.Exec(ctx, query, workerID, consumerName); err != nil {
			return fmt.Errorf("set assignment for consumer %s: %w", consumerName, err)
		}
	}

	return nil
}
