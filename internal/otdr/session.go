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
}

func NewSessionService(registry *topology.Registry, scanner *landing.Scanner, ids platform.IDGenerator, now func() time.Time) *SessionService {
	return &SessionService{
		topology: registry, scanner: scanner, ids: ids, now: now, traces: make(map[string]Trace),
	}
}

func (s *SessionService) Run(ctx context.Context, spanID, stationID string, lengthKM float64, chunks int) (Trace, error) {
	snapshot, err := s.topology.Snapshot(spanID)
	if err != nil {
		return Trace{}, err
	}
	trace := Trace{
		ID: s.ids.New("scan"), SpanID: spanID, StationID: stationID, Generation: snapshot.Generation,
		Path: snapshot.ActivePath, Status: StatusRunning, StartedAt: s.now().UTC(),
	}
	s.mu.Lock()
	s.traces[trace.ID] = trace
	s.mu.Unlock()
	completed := false
	defer func() {
		if !completed {
			s.mu.Lock()
			delete(s.traces, trace.ID)
			s.mu.Unlock()
		}
	}()
	parts, err := s.scanner.ScanPinned(ctx, snapshot, stationID, chunks)
	if err != nil {
		return Trace{}, fmt.Errorf("collect OTDR chunks: %w", err)
	}
	trace, err = assemble(trace, parts, lengthKM, s.now())
	if err != nil {
		return Trace{}, err
	}
	if len(trace.Generations()) != 1 {
		return Trace{}, fmt.Errorf("OTDR trace contains multiple topology generations")
	}
	trace.Status = StatusCompleted
	s.mu.Lock()
	s.traces[trace.ID] = trace
	s.mu.Unlock()
	completed = true
	return trace, nil
}

func (s *SessionService) Get(id string) (Trace, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	trace, exists := s.traces[id]
	return trace, exists
}
