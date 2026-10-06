package otdr

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/wyw14/cry-103/internal/landing"
	"github.com/wyw14/cry-103/internal/platform"
	"github.com/wyw14/cry-103/internal/topology"
)

type SessionService struct {
	mu       sync.RWMutex
	topology *topology.Registry
	scanner  *landing.Scanner
	ids      platform.IDGenerator
	now      func() time.Time
	traces   map[string]Trace
	// order holds scan IDs in registration sequence. seq is a monotonic tie
	// breaker for scans sharing a started_at timestamp, so keyset pagination
	// stays deterministic even when the clock does not advance.
	order []string
	seq   uint64
}

func NewSessionService(registry *topology.Registry, scanner *landing.Scanner, ids platform.IDGenerator, now func() time.Time) *SessionService {
	return &SessionService{
		topology: registry, scanner: scanner, ids: ids, now: now, traces: make(map[string]Trace),
	}
}

// Run registers the scan as running before collection so it is immediately
// visible to listings, then transitions it to completed or failed. The
// returned trace is nil-valued on failure; the failed record remains
// retrievable through Get and the query API.
func (s *SessionService) Run(ctx context.Context, spanID, stationID string, lengthKM float64, chunks int) (Trace, error) {
	snapshot, err := s.topology.Snapshot(spanID)
	if err != nil {
		return Trace{}, err
	}
	trace := Trace{
		ID: s.ids.New("scan"), SpanID: spanID, StationID: stationID,
		Generation: snapshot.Generation, Path: snapshot.ActivePath,
		State: StateRunning, StartedAt: s.now().UTC(),
	}
	s.mu.Lock()
	s.seq++
	trace.sequence = s.seq
	s.traces[trace.ID] = trace
	s.order = append(s.order, trace.ID)
	s.mu.Unlock()

	parts, err := s.scanner.ScanPinned(ctx, snapshot, stationID, chunks)
	if err != nil {
		return Trace{}, s.fail(trace.ID, fmt.Errorf("collect OTDR chunks: %w", err))
	}
	assembled, err := assemble(trace, parts, lengthKM, s.now())
	if err != nil {
		return Trace{}, s.fail(trace.ID, err)
	}
	if len(assembled.Generations()) != 1 {
		return Trace{}, s.fail(trace.ID, fmt.Errorf("OTDR trace contains multiple topology generations"))
	}
	assembled.State = StateCompleted
	s.mu.Lock()
	assembled.sequence = trace.sequence
	s.traces[assembled.ID] = assembled
	s.mu.Unlock()
	return assembled, nil
}

func (s *SessionService) fail(id string, cause error) error {
	s.mu.Lock()
	trace := s.traces[id]
	trace.State = StateFailed
	trace.Error = cause.Error()
	s.traces[id] = trace
	s.mu.Unlock()
	return cause
}

func (s *SessionService) Get(id string) (Trace, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	trace, exists := s.traces[id]
	if !exists {
		return Trace{}, false
	}
	return trace.Snapshot(), true
}

// all returns every scan in registration sequence. Callers must not mutate the
// returned traces; each entry is copied so concurrent state transitions cannot
// tear the view mid-query.
func (s *SessionService) all() []Trace {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]Trace, 0, len(s.order))
	for _, id := range s.order {
		result = append(result, s.traces[id].Snapshot())
	}
	return result
}
