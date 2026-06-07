package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// TryAcquireLease attempts to acquire or renew the leadership lease for the worker.
// It returns true if the lease was successfully acquired or renewed.
func TryAcquireLease(ctx context.Context, db DBTX, table string, workerID uuid.UUID, leaseDuration time.Duration) (bool, error) {
	table = resolveTableName(table, DefaultLeaderElectionTable)

	// We use INSERT ... ON CONFLICT DO UPDATE with an alias 'target' to ensure safety
	// and compatibility with schema-qualified names.
	//nolint:gosec // Table name is resolved from configuration.
	query := fmt.Sprintf(`
		INSERT INTO %s AS target (lease_key, leader_id, expires_at, updated_at)
		VALUES ('leader', $1, NOW() + $2 * INTERVAL '1 microsecond', NOW())
		ON CONFLICT (lease_key) DO UPDATE
		SET leader_id = EXCLUDED.leader_id,
		    expires_at = EXCLUDED.expires_at,
		    updated_at = NOW()
		WHERE target.leader_id = EXCLUDED.leader_id
		   OR target.leader_id IS NULL
		   OR target.expires_at < NOW()
	`, table)

	res, err := db.ExecContext(ctx, query, workerID, leaseDuration.Microseconds())
	if err != nil {
		return false, fmt.Errorf("execute acquire lease for %s: %w", workerID, err)
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("rows affected acquire lease for %s: %w", workerID, err)
	}

	return affected == 1, nil
}

// ReleaseLease voluntarily releases the leadership lease by expiring it and setting
// the leader ID to uuid.Nil.
func ReleaseLease(ctx context.Context, db DBTX, table string, workerID uuid.UUID) error {
	table = resolveTableName(table, DefaultLeaderElectionTable)

	//nolint:gosec // Table name is resolved from configuration.
	query := fmt.Sprintf(`
		UPDATE %s
		SET leader_id = NULL,
		    expires_at = NOW() - INTERVAL '1 minute',
		    updated_at = NOW()
		WHERE lease_key = 'leader' AND leader_id = $1
	`, table)

	if _, err := db.ExecContext(ctx, query, workerID); err != nil {
		return fmt.Errorf("release lease for %s: %w", workerID, err)
	}

	return nil
}

// GetLease retrieves the current leader ID and expiration time from the lease table.
func GetLease(ctx context.Context, db DBTX, table string) (uuid.UUID, time.Time, error) {
	table = resolveTableName(table, DefaultLeaderElectionTable)

	//nolint:gosec // Table name is resolved from configuration.
	query := fmt.Sprintf(`
		SELECT leader_id, expires_at
		FROM %s
		WHERE lease_key = 'leader'
	`, table)

	var leaderID uuid.NullUUID
	var expiresAt time.Time
	err := db.QueryRowContext(ctx, query).Scan(&leaderID, &expiresAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return uuid.Nil, time.Time{}, nil
		}
		return uuid.Nil, time.Time{}, fmt.Errorf("get lease: %w", err)
	}

	if !leaderID.Valid {
		return uuid.Nil, expiresAt, nil
	}
	return leaderID.UUID, expiresAt, nil
}
