package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// GetCheckpoint returns the last processed global position for a consumer.
// It returns 0 if no checkpoint exists.
func GetCheckpoint(ctx context.Context, db DB, table, consumerName string) (int64, error) {
	table = resolveTableName(table, DefaultConsumerCheckpointsTable)

	//nolint:gosec // G201: table name comes from trusted configuration.
	query := fmt.Sprintf(`
		SELECT last_position
		FROM %s
		WHERE consumer_name = $1
	`, table)

	var position int64
	err := db.QueryRow(ctx, query, consumerName).Scan(&position)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, nil
		}
		return 0, fmt.Errorf("get checkpoint for consumer %s: %w", consumerName, err)
	}

	return position, nil
}

// GetCheckpointForUpdate returns the last processed global position for a
// consumer while locking the checkpoint row for update.
func GetCheckpointForUpdate(ctx context.Context, tx pgx.Tx, table, consumerName string) (int64, error) {
	table = resolveTableName(table, DefaultConsumerCheckpointsTable)

	//nolint:gosec // G201: table name comes from trusted configuration.
	query := fmt.Sprintf(`
		SELECT last_position
		FROM %s
		WHERE consumer_name = $1
		FOR UPDATE
	`, table)

	var position int64
	err := tx.QueryRow(ctx, query, consumerName).Scan(&position)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, nil
		}
		return 0, fmt.Errorf("lock checkpoint for consumer %s: %w", consumerName, err)
	}

	return position, nil
}

// SaveCheckpoint upserts the checkpoint for a consumer within the given transaction.
func SaveCheckpoint(ctx context.Context, tx pgx.Tx, table, consumerName string, position int64) error {
	table = resolveTableName(table, DefaultConsumerCheckpointsTable)

	//nolint:gosec // G201: table name comes from trusted configuration.
	query := fmt.Sprintf(`
		INSERT INTO %s (consumer_name, last_position, created_at, updated_at)
		VALUES ($1, $2, NOW(), NOW())
		ON CONFLICT (consumer_name)
		DO UPDATE SET
			last_position = GREATEST(%s.last_position, EXCLUDED.last_position),
			updated_at = NOW()
	`, table, table)

	if _, err := tx.Exec(ctx, query, consumerName, position); err != nil {
		return fmt.Errorf("save checkpoint for consumer %s: %w", consumerName, err)
	}

	return nil
}

// EnsureCheckpointExists creates a checkpoint row if it doesn't exist (position 0).
func EnsureCheckpointExists(ctx context.Context, db DB, table, consumerName string) error {
	table = resolveTableName(table, DefaultConsumerCheckpointsTable)

	//nolint:gosec // G201: table name comes from trusted configuration.
	query := fmt.Sprintf(`
		INSERT INTO %s (consumer_name, last_position, created_at, updated_at)
		VALUES ($1, 0, NOW(), NOW())
		ON CONFLICT (consumer_name) DO NOTHING
	`, table)

	if _, err := db.Exec(ctx, query, consumerName); err != nil {
		return fmt.Errorf("ensure checkpoint for consumer %s: %w", consumerName, err)
	}

	return nil
}
