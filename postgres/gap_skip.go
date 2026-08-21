package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// ProjectionGapSkipRecord captures a durable record of a stale-gap advancement.
type ProjectionGapSkipRecord struct {
	FirstSeenAt            time.Time
	ProjectionName         string
	InstanceID             uuid.UUID
	GapPosition            int64
	SkipToPosition         int64
	HighestVisiblePosition int64
}

// RecordGapSkip stores a durable audit record for a stale-gap advancement.
func RecordGapSkip(ctx context.Context, tx pgx.Tx, table string, record *ProjectionGapSkipRecord) error {
	table = resolveTableName(table, DefaultProjectionGapSkipsTable)

	//nolint:gosec // G201: table name comes from trusted configuration.
	query := fmt.Sprintf(`
		INSERT INTO %s (
			projection_name,
			instance_id,
			gap_position,
			skip_to_position,
			highest_visible_position,
			first_seen_at,
			created_at
		) VALUES ($1, $2, $3, $4, $5, $6, NOW())
	`, table)

	if _, err := tx.Exec(
		ctx,
		query,
		record.ProjectionName,
		record.InstanceID,
		record.GapPosition,
		record.SkipToPosition,
		record.HighestVisiblePosition,
		record.FirstSeenAt,
	); err != nil {
		return fmt.Errorf("record gap skip for projection %s: %w", record.ProjectionName, err)
	}

	return nil
}
