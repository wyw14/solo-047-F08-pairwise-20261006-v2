package otdr

import (
	"errors"
	"time"

	"github.com/wyw14/cry-103/internal/landing"
	"github.com/wyw14/cry-103/internal/span"
	"github.com/wyw14/cry-103/internal/topology"
)

// ScanState classifies where a scan is in its lifecycle. Running scans are
// registered before collection starts so they are visible while in progress.
type ScanState string

const (
	// StateRunning means the scan has been registered but collection is not finished.
	StateRunning ScanState = "running"
	// StateCompleted means the trace was assembled and persisted successfully.
	StateCompleted ScanState = "completed"
	// StateFailed means collection or assembly failed; Error explains why.
	StateFailed ScanState = "failed"
)

type Trace struct {
	ID          string               `json:"id"`
	SpanID      string               `json:"span_id"`
	StationID   string               `json:"station_id"`
	Generation  uint64               `json:"generation"`
	Path        topology.Path        `json:"path"`
	State       ScanState            `json:"state"`
	Chunks      []landing.TraceChunk `json:"chunks,omitempty"`
	Location    span.FaultLocation   `json:"location"`
	StartedAt   time.Time            `json:"started_at"`
	CompletedAt *time.Time           `json:"completed_at,omitempty"`
	// Error holds the failure reason and is only populated for failed scans.
	Error string `json:"error,omitempty"`
	// sequence is the monotonic registration order used as a tie breaker for
	// equal StartedAt values in keyset pagination; never serialized.
	sequence uint64 `json:"-"`
}

// Snapshot returns a copy of the trace safe for the caller to retain while the
// service mutates the stored entry.
func (t Trace) Snapshot() Trace {
	trace := t
	if t.Chunks != nil {
		trace.Chunks = append([]landing.TraceChunk(nil), t.Chunks...)
	}
	return trace
}

func assemble(trace Trace, chunks []landing.TraceChunk, lengthKM float64, completedAt time.Time) (Trace, error) {
	if len(chunks) == 0 {
		return Trace{}, errors.New("OTDR trace has no chunks")
	}
	reflections := make([]span.Reflection, 0, len(chunks))
	for _, chunk := range chunks {
		if chunk.Generation != trace.Generation || chunk.Path != trace.Path {
			return Trace{}, errors.New("OTDR chunks cross topology generation")
		}
		reflections = append(reflections, span.Reflection{
			DistanceKM: chunk.DistanceKM, LossDB: chunk.LossDB, Direction: chunk.StationID,
		})
	}
	location, err := span.Locate(lengthKM, reflections)
	if err != nil {
		return Trace{}, err
	}
	trace.Chunks = append([]landing.TraceChunk(nil), chunks...)
	trace.Location = location
	completed := completedAt.UTC()
	trace.CompletedAt = &completed
	return trace, nil
}

func (t Trace) Generations() []uint64 {
	seen := make(map[uint64]bool)
	var result []uint64
	for _, chunk := range t.Chunks {
		if !seen[chunk.Generation] {
			seen[chunk.Generation] = true
			result = append(result, chunk.Generation)
		}
	}
	return result
}
