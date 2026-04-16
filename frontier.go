package worker

import (
	"time"

	"github.com/eventsalsa/store"
)

var timeNow = time.Now

type gapState struct {
	firstSeenAt     time.Time
	missingPosition int64
	highestVisible  int64
	staleLogged     bool
}

func (s *gapState) observe(missingPosition, highestVisible int64, now time.Time) time.Duration {
	if s.missingPosition != missingPosition || s.firstSeenAt.IsZero() {
		s.missingPosition = missingPosition
		s.firstSeenAt = now
		s.highestVisible = highestVisible
		s.staleLogged = false
		return 0
	}

	if highestVisible > s.highestVisible {
		s.highestVisible = highestVisible
	}

	return now.Sub(s.firstSeenAt)
}

func (s *gapState) clear() {
	*s = gapState{}
}

func contiguousPrefixCount(expected int64, events []store.PersistedEvent) int {
	count := 0
	for i := range events {
		if events[i].GlobalPosition != expected {
			break
		}

		count++
		expected++
	}

	return count
}

func computeSafeFrontier(checkpoint int64, events []store.PersistedEvent) (count int, frontier int64) {
	count = contiguousPrefixCount(checkpoint+1, events)
	if count == 0 {
		return 0, checkpoint
	}

	return count, events[count-1].GlobalPosition
}

func computeSafeHarbor(expected int64, events []store.PersistedEvent, lag int) (frontier int64, ok bool) {
	if len(events) == 0 {
		return 0, false
	}
	if lag < 0 {
		lag = 0
	}
	if maxLag := len(events) - 1; lag > maxLag {
		lag = maxLag
	}

	index := len(events) - 1 - lag

	count := contiguousPrefixCount(expected, events)
	if count == 0 {
		return 0, false
	}

	frontier = events[index].GlobalPosition
	contiguousFrontier := events[count-1].GlobalPosition
	if frontier > contiguousFrontier {
		frontier = contiguousFrontier
	}

	return frontier, frontier >= expected
}

func computeGapSkipTarget(gapPosition int64, events []store.PersistedEvent, lag int) (frontier int64, ok bool) {
	frontier, ok = computeSafeHarbor(gapPosition+1, events, lag)
	if ok || len(events) == 0 || events[0].GlobalPosition <= gapPosition+1 {
		return frontier, ok
	}

	return computeSafeHarbor(events[0].GlobalPosition, events, lag)
}
