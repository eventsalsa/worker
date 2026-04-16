package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// GapSkipRecord captures a durable record of a stale-gap advancement.
type GapSkipRecord struct {
	FirstSeenAt            time.Time
	ConsumerName           string
	WorkerID               uuid.UUID
	GapPosition            int64
	SkipToPosition         int64
	HighestVisiblePosition int64
}

// RecordGapSkip stores a durable audit record for a stale-gap advancement.
func RecordGapSkip(ctx context.Context, db DBTX, table string, record *GapSkipRecord) error {
	table = resolveTableName(table, DefaultConsumerGapSkipsTable)

	//nolint:gosec // G201: table name comes from trusted configuration.
	query := fmt.Sprintf(`
		INSERT INTO %s (
			consumer_name,
			worker_id,
			gap_position,
			skip_to_position,
			highest_visible_position,
			first_seen_at,
			created_at
		) VALUES ($1, $2, $3, $4, $5, $6, NOW())
	`, table)

	if _, err := db.ExecContext(
		ctx,
		query,
		record.ConsumerName,
		record.WorkerID,
		record.GapPosition,
		record.SkipToPosition,
		record.HighestVisiblePosition,
		record.FirstSeenAt,
	); err != nil {
		return fmt.Errorf("record gap skip for consumer %s: %w", record.ConsumerName, err)
	}

	return nil
}
