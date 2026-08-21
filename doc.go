// Package projector provides horizontally scalable, PostgreSQL-native projection
// processing infrastructure for event-sourced systems.
//
// It builds on github.com/eventsalsa/store for event definitions, logging,
// and event store access while adding projector coordination,
// projection assignment, checkpointing, and processing infrastructure.
package projector
