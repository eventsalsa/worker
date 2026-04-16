package migrations

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	if config.OutputFolder != "migrations" {
		t.Fatalf("OutputFolder = %q, want %q", config.OutputFolder, "migrations")
	}

	outputFilenamePattern := regexp.MustCompile(`^\d{14}_init_worker_infrastructure\.sql$`)
	if !outputFilenamePattern.MatchString(config.OutputFilename) {
		t.Fatalf("OutputFilename = %q, want timestamped worker migration filename", config.OutputFilename)
	}

	if config.WorkerNodesTable != "worker_nodes" {
		t.Fatalf("WorkerNodesTable = %q, want %q", config.WorkerNodesTable, "worker_nodes")
	}

	if config.ConsumerAssignmentsTable != "consumer_assignments" {
		t.Fatalf("ConsumerAssignmentsTable = %q, want %q", config.ConsumerAssignmentsTable, "consumer_assignments")
	}

	if config.ConsumerCheckpointsTable != "consumer_checkpoints" {
		t.Fatalf("ConsumerCheckpointsTable = %q, want %q", config.ConsumerCheckpointsTable, "consumer_checkpoints")
	}
	if config.ConsumerGapSkipsTable != "consumer_gap_skips" {
		t.Fatalf("ConsumerGapSkipsTable = %q, want %q", config.ConsumerGapSkipsTable, "consumer_gap_skips")
	}
}

func TestGeneratePostgresSQL(t *testing.T) {
	config := Config{
		OutputFolder:             t.TempDir(),
		OutputFilename:           "worker_migration.sql",
		WorkerNodesTable:         "worker_nodes",
		ConsumerAssignmentsTable: "consumer_assignments",
		ConsumerCheckpointsTable: "consumer_checkpoints",
		ConsumerGapSkipsTable:    "consumer_gap_skips",
	}

	sql := generatePostgresSQL(&config)

	requiredStrings := []string{
		"-- Worker Infrastructure Migration",
		"CREATE TABLE IF NOT EXISTS worker_nodes",
		"CREATE TABLE IF NOT EXISTS consumer_assignments",
		"CREATE TABLE IF NOT EXISTS consumer_checkpoints",
		"CREATE TABLE IF NOT EXISTS consumer_gap_skips",
		"worker_id UUID PRIMARY KEY",
		"heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT NOW()",
		"created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()",
		"worker_id UUID REFERENCES worker_nodes(worker_id) ON DELETE SET NULL",
		"last_position BIGINT NOT NULL DEFAULT 0",
		"skip_to_position BIGINT NOT NULL",
		"updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()",
	}

	for _, required := range requiredStrings {
		if !strings.Contains(sql, required) {
			t.Errorf("Generated SQL missing required string: %s", required)
		}
	}

	requiredIndexes := []string{
		"CREATE INDEX IF NOT EXISTS idx_worker_nodes_heartbeat",
		"ON worker_nodes (heartbeat_at)",
		"CREATE INDEX IF NOT EXISTS idx_consumer_assignments_worker",
		"ON consumer_assignments (worker_id)",
		"CREATE INDEX IF NOT EXISTS idx_consumer_gap_skips_consumer",
		"ON consumer_gap_skips (consumer_name, created_at DESC)",
	}

	for _, required := range requiredIndexes {
		if !strings.Contains(sql, required) {
			t.Errorf("Generated SQL missing required index definition: %s", required)
		}
	}
}

func TestGeneratePostgresSQL_CustomTableNames(t *testing.T) {
	config := Config{
		OutputFolder:             t.TempDir(),
		OutputFilename:           "custom_worker_migration.sql",
		WorkerNodesTable:         "custom_worker_nodes",
		ConsumerAssignmentsTable: "custom_consumer_assignments",
		ConsumerCheckpointsTable: "custom_consumer_checkpoints",
		ConsumerGapSkipsTable:    "custom_consumer_gap_skips",
	}

	sql := generatePostgresSQL(&config)

	requiredStrings := []string{
		"CREATE TABLE IF NOT EXISTS custom_worker_nodes",
		"CREATE TABLE IF NOT EXISTS custom_consumer_assignments",
		"CREATE TABLE IF NOT EXISTS custom_consumer_checkpoints",
		"CREATE TABLE IF NOT EXISTS custom_consumer_gap_skips",
		"worker_id UUID REFERENCES custom_worker_nodes(worker_id) ON DELETE SET NULL",
		"CREATE INDEX IF NOT EXISTS idx_custom_worker_nodes_heartbeat",
		"CREATE INDEX IF NOT EXISTS idx_custom_consumer_assignments_worker",
		"CREATE INDEX IF NOT EXISTS idx_custom_consumer_gap_skips_consumer",
	}

	for _, required := range requiredStrings {
		if !strings.Contains(sql, required) {
			t.Errorf("Generated SQL missing custom configuration string: %s", required)
		}
	}
}

func TestGeneratePostgresSQL_DefaultsMissingGapSkipTable(t *testing.T) {
	config := Config{
		OutputFolder:             t.TempDir(),
		OutputFilename:           "worker_migration.sql",
		WorkerNodesTable:         "worker_nodes",
		ConsumerAssignmentsTable: "consumer_assignments",
		ConsumerCheckpointsTable: "consumer_checkpoints",
	}

	sql := generatePostgresSQL(&config)

	requiredStrings := []string{
		"CREATE TABLE IF NOT EXISTS consumer_gap_skips",
		"CREATE INDEX IF NOT EXISTS idx_consumer_gap_skips_consumer",
		"ON consumer_gap_skips (consumer_name, created_at DESC)",
	}

	for _, required := range requiredStrings {
		if !strings.Contains(sql, required) {
			t.Errorf("Generated SQL missing default gap-skip string: %s", required)
		}
	}
}

func TestGeneratePostgresSQL_SchemaQualifiedTableNames(t *testing.T) {
	config := Config{
		OutputFolder:             t.TempDir(),
		OutputFilename:           "schema_worker_migration.sql",
		WorkerNodesTable:         "infra.worker_nodes",
		ConsumerAssignmentsTable: "infra.consumer_assignments",
		ConsumerCheckpointsTable: "infra.consumer_checkpoints",
		ConsumerGapSkipsTable:    "infra.consumer_gap_skips",
	}

	sql := generatePostgresSQL(&config)

	requiredStrings := []string{
		"CREATE SCHEMA IF NOT EXISTS infra;",
		"CREATE TABLE IF NOT EXISTS infra.worker_nodes",
		"CREATE INDEX IF NOT EXISTS idx_worker_nodes_heartbeat",
		"ON infra.worker_nodes (heartbeat_at)",
		"CREATE INDEX IF NOT EXISTS idx_consumer_assignments_worker",
		"ON infra.consumer_assignments (worker_id)",
		"CREATE INDEX IF NOT EXISTS idx_consumer_gap_skips_consumer",
		"ON infra.consumer_gap_skips (consumer_name, created_at DESC)",
	}

	for _, required := range requiredStrings {
		if !strings.Contains(sql, required) {
			t.Errorf("Generated SQL missing schema-qualified string: %s", required)
		}
	}
}

func TestGeneratePostgres_WritesFile(t *testing.T) {
	tmpDir := t.TempDir()

	config := Config{
		OutputFolder:             tmpDir,
		OutputFilename:           "test_worker_migration.sql",
		WorkerNodesTable:         "worker_nodes",
		ConsumerAssignmentsTable: "consumer_assignments",
		ConsumerCheckpointsTable: "consumer_checkpoints",
		ConsumerGapSkipsTable:    "consumer_gap_skips",
	}

	if err := GeneratePostgres(&config); err != nil {
		t.Fatalf("GeneratePostgres failed: %v", err)
	}

	outputPath := filepath.Join(tmpDir, config.OutputFilename)
	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("Failed to read generated file: %v", err)
	}

	sql := string(content)
	if sql != generatePostgresSQL(&config) {
		t.Fatal("generated file contents do not match generated SQL")
	}
}
