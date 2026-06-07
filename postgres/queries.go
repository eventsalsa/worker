package postgres

import (
	"context"
	"database/sql"
	"strings"
)

// DBTX abstracts *sql.DB and *sql.Tx for functions that work with either.
type DBTX interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

// Default table names for worker infrastructure metadata.
const (
	DefaultWorkerNodesTable         = "worker_nodes"
	DefaultConsumerAssignmentsTable = "consumer_assignments"
	DefaultConsumerCheckpointsTable = "consumer_checkpoints"
	DefaultConsumerGapSkipsTable    = "consumer_gap_skips"
	DefaultLeaderElectionTable      = "worker_leader_election"
)

func resolveTableName(tableName, defaultTableName string) string {
	if strings.TrimSpace(tableName) == "" {
		return defaultTableName
	}

	return tableName
}
