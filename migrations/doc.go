// Package migrations provides DDL generation for worker meta tables.
//
// To generate migrations with the stable CLI entrypoint, use:
//
//	go run github.com/eventsalsa/worker/cmd/migrate-gen -output migrations
//
// Or add a go generate directive to your code:
//
//	//go:generate go run github.com/eventsalsa/worker/cmd/migrate-gen -output ../../migrations
//
// For advanced cases, call GeneratePostgres with a Config value from your own
// program so you can override filenames or table names directly.
package migrations

//go:generate go run ../cmd/migrate-gen -output example_migrations -filename example.sql
