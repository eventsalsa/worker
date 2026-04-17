package main

import (
	"bytes"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestRunGeneratesMigrationWithDefaults(t *testing.T) {
	outputDir := t.TempDir()
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	exitCode := run([]string{"-output", outputDir}, &stdout, &stderr)
	if exitCode != 0 {
		t.Fatalf("run exit code = %d, want 0; stderr=%q", exitCode, stderr.String())
	}

	entries, err := os.ReadDir(outputDir)
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("generated files = %d, want 1", len(entries))
	}

	outputFilename := entries[0].Name()
	if !regexp.MustCompile(`^\d{14}_init_worker_infrastructure\.sql$`).MatchString(outputFilename) {
		t.Fatalf("output filename = %q, want timestamped worker migration filename", outputFilename)
	}

	content, err := os.ReadFile(filepath.Join(outputDir, outputFilename))
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}

	sql := string(content)
	requiredStrings := []string{
		"CREATE TABLE IF NOT EXISTS worker_nodes",
		"CREATE TABLE IF NOT EXISTS consumer_assignments",
		"CREATE TABLE IF NOT EXISTS consumer_checkpoints",
		"CREATE TABLE IF NOT EXISTS consumer_gap_skips",
	}
	for _, required := range requiredStrings {
		if !strings.Contains(sql, required) {
			t.Fatalf("generated SQL missing %q", required)
		}
	}

	expectedOutput := "Generated PostgreSQL migration: " + filepath.Join(outputDir, outputFilename) + "\n"
	if stdout.String() != expectedOutput {
		t.Fatalf("stdout = %q, want %q", stdout.String(), expectedOutput)
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestRunGeneratesMigrationWithOverrides(t *testing.T) {
	outputDir := t.TempDir()
	outputFilename := "002_worker_tables.sql"
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	exitCode := run([]string{
		"-output", outputDir,
		"-filename", outputFilename,
		"-worker-nodes-table", "infra.worker_nodes",
		"-consumer-assignments-table", "infra.consumer_assignments",
		"-consumer-checkpoints-table", "infra.consumer_checkpoints",
		"-consumer-gap-skips-table", "infra.consumer_gap_skips",
	}, &stdout, &stderr)
	if exitCode != 0 {
		t.Fatalf("run exit code = %d, want 0; stderr=%q", exitCode, stderr.String())
	}

	content, err := os.ReadFile(filepath.Join(outputDir, outputFilename))
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}

	sql := string(content)
	requiredStrings := []string{
		"CREATE SCHEMA IF NOT EXISTS infra;",
		"CREATE TABLE IF NOT EXISTS infra.worker_nodes",
		"CREATE TABLE IF NOT EXISTS infra.consumer_assignments",
		"CREATE TABLE IF NOT EXISTS infra.consumer_checkpoints",
		"CREATE TABLE IF NOT EXISTS infra.consumer_gap_skips",
	}
	for _, required := range requiredStrings {
		if !strings.Contains(sql, required) {
			t.Fatalf("generated SQL missing %q", required)
		}
	}

	expectedOutput := "Generated PostgreSQL migration: " + filepath.Join(outputDir, outputFilename) + "\n"
	if stdout.String() != expectedOutput {
		t.Fatalf("stdout = %q, want %q", stdout.String(), expectedOutput)
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestRunReturnsParseError(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	exitCode := run([]string{"-does-not-exist"}, &stdout, &stderr)
	if exitCode != 2 {
		t.Fatalf("run exit code = %d, want 2", exitCode)
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want empty", stdout.String())
	}
	if !strings.Contains(stderr.String(), "flag provided but not defined") {
		t.Fatalf("stderr = %q, want parse error", stderr.String())
	}
}
