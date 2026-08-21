package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// GetCheckpoint returns the last processed global position for a projection.
// It returns 0 if no checkpoint exists.
func GetCheckpoint(ctx context.Context, db DB, table, projectionName string) (int64, error) {
	table = resolveTableName(table, DefaultProjectionCheckpointsTable)

	//nolint:gosec // G201: table name comes from trusted configuration.
	query := fmt.Sprintf(`
		SELECT last_position
		FROM %s
		WHERE projection_name = $1
	`, table)

	var position int64
	err := db.QueryRow(ctx, query, projectionName).Scan(&position)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, nil
		}
		return 0, fmt.Errorf("get checkpoint for projection %s: %w", projectionName, err)
	}

	return position, nil
}

// GetCheckpointForUpdate returns the last processed global position for a
// projection while locking the checkpoint row for update.
func GetCheckpointForUpdate(ctx context.Context, tx pgx.Tx, table, projectionName string) (int64, error) {
	table = resolveTableName(table, DefaultProjectionCheckpointsTable)

	//nolint:gosec // G201: table name comes from trusted configuration.
	query := fmt.Sprintf(`
		SELECT last_position
		FROM %s
		WHERE projection_name = $1
		FOR UPDATE
	`, table)

	var position int64
	err := tx.QueryRow(ctx, query, projectionName).Scan(&position)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, nil
		}
		return 0, fmt.Errorf("lock checkpoint for projection %s: %w", projectionName, err)
	}

	return position, nil
}

// SaveCheckpoint upserts the checkpoint for a projection within the given transaction.
func SaveCheckpoint(ctx context.Context, tx pgx.Tx, table, projectionName string, position int64) error {
	table = resolveTableName(table, DefaultProjectionCheckpointsTable)

	//nolint:gosec // G201: table name comes from trusted configuration.
	query := fmt.Sprintf(`
		INSERT INTO %s (projection_name, last_position, created_at, updated_at)
		VALUES ($1, $2, NOW(), NOW())
		ON CONFLICT (projection_name)
		DO UPDATE SET
			last_position = GREATEST(%s.last_position, EXCLUDED.last_position),
			updated_at = NOW()
	`, table, table)

	if _, err := tx.Exec(ctx, query, projectionName, position); err != nil {
		return fmt.Errorf("save checkpoint for projection %s: %w", projectionName, err)
	}

	return nil
}

// EnsureCheckpointExists creates a checkpoint row if it doesn't exist (position 0).
func EnsureCheckpointExists(ctx context.Context, db DB, table, projectionName string) error {
	table = resolveTableName(table, DefaultProjectionCheckpointsTable)

	//nolint:gosec // G201: table name comes from trusted configuration.
	query := fmt.Sprintf(`
		INSERT INTO %s (projection_name, last_position, created_at, updated_at)
		VALUES ($1, 0, NOW(), NOW())
		ON CONFLICT (projection_name) DO NOTHING
	`, table)

	if _, err := db.Exec(ctx, query, projectionName); err != nil {
		return fmt.Errorf("ensure checkpoint for projection %s: %w", projectionName, err)
	}

	return nil
}
