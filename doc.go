// Package worker provides horizontally scalable, PostgreSQL-native consumer
// processing infrastructure for event-sourced systems.
//
// It builds on github.com/eventsalsa/store for event definitions, consumer
// contracts, logging, and event store access while adding worker coordination,
// consumer assignment, checkpointing, and processing infrastructure.
package worker
