// Command migrate-gen generates SQL migration files for worker infrastructure.
//
// Usage:
//
//	go run github.com/eventsalsa/worker/cmd/migrate-gen -output migrations -filename init_worker.sql
//
// Or with go generate:
//
//	//go:generate go run github.com/eventsalsa/worker/cmd/migrate-gen -output migrations
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/eventsalsa/worker/migrations"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	config := migrations.DefaultConfig()

	flags := flag.NewFlagSet("migrate-gen", flag.ContinueOnError)
	flags.SetOutput(stderr)

	outputFolder := flags.String("output", config.OutputFolder, "Output folder for migration file")
	outputFilename := flags.String("filename", "", "Output filename (default: timestamp-based)")
	workerNodesTable := flags.String("worker-nodes-table", config.WorkerNodesTable, "Name of worker nodes table")
	consumerAssignmentsTable := flags.String("consumer-assignments-table", config.ConsumerAssignmentsTable, "Name of consumer assignments table")
	consumerCheckpointsTable := flags.String("consumer-checkpoints-table", config.ConsumerCheckpointsTable, "Name of consumer checkpoints table")
	consumerGapSkipsTable := flags.String("consumer-gap-skips-table", config.ConsumerGapSkipsTable, "Name of consumer gap skips table")

	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}

		return 2
	}

	config.OutputFolder = *outputFolder
	config.WorkerNodesTable = *workerNodesTable
	config.ConsumerAssignmentsTable = *consumerAssignmentsTable
	config.ConsumerCheckpointsTable = *consumerCheckpointsTable
	config.ConsumerGapSkipsTable = *consumerGapSkipsTable

	if *outputFilename != "" {
		config.OutputFilename = *outputFilename
	}

	if err := migrations.GeneratePostgres(&config); err != nil {
		fmt.Fprintf(stderr, "Error generating migration: %v\n", err)
		return 1
	}

	fmt.Fprintf(stdout, "Generated PostgreSQL migration: %s\n", filepath.Join(config.OutputFolder, config.OutputFilename))
	return 0
}
