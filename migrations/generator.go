package migrations

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Config configures worker migration generation.
type Config struct {
	// OutputFolder is the directory where the migration file will be written.
	OutputFolder string

	// OutputFilename is the name of the migration file.
	OutputFilename string

	// WorkerNodesTable is the name of the worker registration table.
	WorkerNodesTable string

	// ConsumerAssignmentsTable is the name of the consumer assignment table.
	ConsumerAssignmentsTable string

	// ConsumerCheckpointsTable is the name of the consumer checkpoint table.
	ConsumerCheckpointsTable string

	// ConsumerGapSkipsTable is the name of the consumer gap skip audit table.
	ConsumerGapSkipsTable string
}

// DefaultConfig returns the default configuration.
func DefaultConfig() Config {
	timestamp := time.Now().Format("20060102150405")

	return Config{
		OutputFolder:             "migrations",
		OutputFilename:           fmt.Sprintf("%s_init_worker_infrastructure.sql", timestamp),
		WorkerNodesTable:         "worker_nodes",
		ConsumerAssignmentsTable: "consumer_assignments",
		ConsumerCheckpointsTable: "consumer_checkpoints",
		ConsumerGapSkipsTable:    "consumer_gap_skips",
	}
}

// GeneratePostgres generates a PostgreSQL migration file.
func GeneratePostgres(config *Config) error {
	normalized := normalizeConfig(config)

	if err := os.MkdirAll(normalized.OutputFolder, 0o755); err != nil {
		return fmt.Errorf("failed to create output folder: %w", err)
	}

	sql := generatePostgresSQL(&normalized)

	outputPath := filepath.Join(normalized.OutputFolder, normalized.OutputFilename)
	if err := os.WriteFile(outputPath, []byte(sql), 0o600); err != nil {
		return fmt.Errorf("failed to write migration file: %w", err)
	}

	return nil
}

func generatePostgresSQL(config *Config) string {
	normalized := normalizeConfig(config)

	schemaDDL := schemaStatements(
		normalized.WorkerNodesTable,
		normalized.ConsumerAssignmentsTable,
		normalized.ConsumerCheckpointsTable,
		normalized.ConsumerGapSkipsTable,
	)

	return fmt.Sprintf(`-- Worker Infrastructure Migration
-- Generated: %s
--
-- These tables coordinate distributed worker execution:
-- - %s tracks worker nodes and heartbeats
-- - %s maps consumers to worker nodes
-- - %s stores each consumer's last processed global position
-- - %s stores durable stale-gap advancement records
%s
CREATE TABLE IF NOT EXISTS %s (
    worker_id UUID PRIMARY KEY,
    heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_%s_heartbeat
    ON %s (heartbeat_at);

CREATE TABLE IF NOT EXISTS %s (
    consumer_name TEXT PRIMARY KEY,
    worker_id UUID REFERENCES %s(worker_id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_%s_worker
    ON %s (worker_id);

CREATE TABLE IF NOT EXISTS %s (
    consumer_name TEXT PRIMARY KEY,
    last_position BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS %s (
    id BIGSERIAL PRIMARY KEY,
    consumer_name TEXT NOT NULL,
    worker_id UUID NOT NULL,
    gap_position BIGINT NOT NULL,
    skip_to_position BIGINT NOT NULL,
    highest_visible_position BIGINT NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_%s_consumer
    ON %s (consumer_name, created_at DESC);
`,
		time.Now().Format(time.RFC3339),
		normalized.WorkerNodesTable,
		normalized.ConsumerAssignmentsTable,
		normalized.ConsumerCheckpointsTable,
		normalized.ConsumerGapSkipsTable,
		schemaDDL,
		normalized.WorkerNodesTable,
		indexNameComponent(normalized.WorkerNodesTable), normalized.WorkerNodesTable,
		normalized.ConsumerAssignmentsTable,
		normalized.WorkerNodesTable,
		indexNameComponent(normalized.ConsumerAssignmentsTable), normalized.ConsumerAssignmentsTable,
		normalized.ConsumerCheckpointsTable,
		normalized.ConsumerGapSkipsTable,
		indexNameComponent(normalized.ConsumerGapSkipsTable), normalized.ConsumerGapSkipsTable,
	)
}

func normalizeConfig(config *Config) Config {
	normalized := DefaultConfig()
	if config == nil {
		return normalized
	}

	if config.OutputFolder != "" {
		normalized.OutputFolder = config.OutputFolder
	}
	if config.OutputFilename != "" {
		normalized.OutputFilename = config.OutputFilename
	}
	if config.WorkerNodesTable != "" {
		normalized.WorkerNodesTable = config.WorkerNodesTable
	}
	if config.ConsumerAssignmentsTable != "" {
		normalized.ConsumerAssignmentsTable = config.ConsumerAssignmentsTable
	}
	if config.ConsumerCheckpointsTable != "" {
		normalized.ConsumerCheckpointsTable = config.ConsumerCheckpointsTable
	}
	if config.ConsumerGapSkipsTable != "" {
		normalized.ConsumerGapSkipsTable = config.ConsumerGapSkipsTable
	}

	return normalized
}

func schemaStatements(tableNames ...string) string {
	seen := make(map[string]struct{})
	var statements []string

	for _, tableName := range tableNames {
		schemaName := schemaName(tableName)
		if schemaName == "" {
			continue
		}
		if _, ok := seen[schemaName]; ok {
			continue
		}

		seen[schemaName] = struct{}{}
		statements = append(statements, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s;\n", schemaName))
	}

	if len(statements) == 0 {
		return ""
	}

	return strings.Join(statements, "")
}

func schemaName(tableName string) string {
	if idx := strings.LastIndex(tableName, "."); idx >= 0 {
		return tableName[:idx]
	}

	return ""
}

func indexNameComponent(tableName string) string {
	base := tableName
	if idx := strings.LastIndex(base, "."); idx >= 0 {
		base = base[idx+1:]
	}

	base = strings.Trim(base, `"`)
	base = strings.ReplaceAll(base, "-", "_")
	base = strings.ReplaceAll(base, " ", "_")

	return base
}
