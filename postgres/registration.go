package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// ErrInstanceRegistrationMissing indicates that a heartbeat update targeted an
// instance row that no longer exists.
var ErrInstanceRegistrationMissing = errors.New("instance registration missing")

// RegisterInstance inserts a new projector instance with the given UUID.
func RegisterInstance(ctx context.Context, db DB, table string, instanceID uuid.UUID) error {
	table = resolveTableName(table, DefaultProjectorInstancesTable)

	//nolint:gosec // G201: table name comes from trusted configuration.
	query := fmt.Sprintf(`
		INSERT INTO %s (instance_id, heartbeat_at, created_at, updated_at)
		VALUES ($1, NOW(), NOW(), NOW())
	`, table)

	if _, err := db.Exec(ctx, query, instanceID); err != nil {
		return fmt.Errorf("register instance %s: %w", instanceID, err)
	}

	return nil
}

// CleanupStaleInstances deletes instance rows older than the given threshold.
func CleanupStaleInstances(ctx context.Context, db DB, table string, olderThan time.Duration) (int64, error) {
	if olderThan <= 0 {
		return 0, fmt.Errorf("cleanup stale instances: olderThan must be positive")
	}

	table = resolveTableName(table, DefaultProjectorInstancesTable)

	//nolint:gosec // G201: table name comes from trusted configuration.
	query := fmt.Sprintf(`
		DELETE FROM %s
		WHERE heartbeat_at < NOW() - ($1 * INTERVAL '1 microsecond')
	`, table)

	result, err := db.Exec(ctx, query, olderThan.Microseconds())
	if err != nil {
		return 0, fmt.Errorf("cleanup stale instances older than %s: %w", olderThan, err)
	}

	affected := result.RowsAffected()
	return affected, nil
}

// UpdateHeartbeat updates the heartbeat timestamp for an instance.
func UpdateHeartbeat(ctx context.Context, db DB, table string, instanceID uuid.UUID) error {
	table = resolveTableName(table, DefaultProjectorInstancesTable)

	//nolint:gosec // G201: table name comes from trusted configuration.
	query := fmt.Sprintf(`
		UPDATE %s
		SET heartbeat_at = NOW(), updated_at = NOW()
		WHERE instance_id = $1
	`, table)

	result, err := db.Exec(ctx, query, instanceID)
	if err != nil {
		return fmt.Errorf("update heartbeat for instance %s: %w", instanceID, err)
	}

	affected := result.RowsAffected()
	if affected == 0 {
		return fmt.Errorf("update heartbeat for instance %s: %w", instanceID, ErrInstanceRegistrationMissing)
	}

	return nil
}

// ListLiveInstances returns instance IDs whose heartbeat is within the timeout threshold.
func ListLiveInstances(ctx context.Context, db DB, table string, timeout time.Duration) ([]uuid.UUID, error) {
	table = resolveTableName(table, DefaultProjectorInstancesTable)

	//nolint:gosec // G201: table name comes from trusted configuration.
	query := fmt.Sprintf(`
		SELECT instance_id
		FROM %s
		WHERE heartbeat_at >= NOW() - ($1 * INTERVAL '1 microsecond')
		ORDER BY instance_id ASC
	`, table)

	rows, err := db.Query(ctx, query, timeout.Microseconds())
	if err != nil {
		return nil, fmt.Errorf("list live instances: %w", err)
	}
	defer rows.Close()

	var instanceIDs []uuid.UUID
	for rows.Next() {
		var instanceID uuid.UUID
		if err := rows.Scan(&instanceID); err != nil {
			return nil, fmt.Errorf("scan live instance: %w", err)
		}
		instanceIDs = append(instanceIDs, instanceID)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate live instances: %w", err)
	}

	return instanceIDs, nil
}

// RemoveInstance deletes an instance row.
func RemoveInstance(ctx context.Context, db DB, table string, instanceID uuid.UUID) error {
	table = resolveTableName(table, DefaultProjectorInstancesTable)

	//nolint:gosec // G201: table name comes from trusted configuration.
	query := fmt.Sprintf(`
		DELETE FROM %s
		WHERE instance_id = $1
	`, table)

	if _, err := db.Exec(ctx, query, instanceID); err != nil {
		return fmt.Errorf("remove instance %s: %w", instanceID, err)
	}

	return nil
}
