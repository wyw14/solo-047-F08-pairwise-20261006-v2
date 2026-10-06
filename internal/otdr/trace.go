package otdr

import (
	"errors"
	"time"

	"github.com/wyw14/cry-103/internal/landing"
	"github.com/wyw14/cry-103/internal/span"
	"github.com/wyw14/cry-103/internal/topology"
)

type TraceStatus string

const (
	StatusRunning   TraceStatus = "running"
	StatusCompleted TraceStatus = "completed"
)

type Trace struct {
	ID          string               `json:"id"`
	SpanID      string               `json:"span_id"`
	StationID   string               `json:"station_id"`
	Generation  uint64               `json:"generation"`
	Path        topology.Path        `json:"path"`
	Status      TraceStatus          `json:"status"`
	Chunks      []landing.TraceChunk `json:"chunks"`
	Location    span.FaultLocation   `json:"location"`
	StartedAt   time.Time            `json:"started_at"`
	CompletedAt time.Time            `json:"completed_at"`
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
	trace.CompletedAt = completedAt.UTC()
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
