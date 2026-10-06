package otdr

import (
	"sort"
	"time"
)

// ScanCursor marks a position in the stable scan ordering: newest first by
// (StartedAt, ID). Both parts are immutable once a scan is recorded, so a
// cursor stays valid while new scans are written.
type ScanCursor struct {
	StartedAt time.Time
	ID        string
}

func scanKey(trace Trace) ScanCursor {
	return ScanCursor{StartedAt: trace.StartedAt, ID: trace.ID}
}

// before reports whether c sorts strictly ahead of other.
func (c ScanCursor) before(other ScanCursor) bool {
	if !c.StartedAt.Equal(other.StartedAt) {
		return c.StartedAt.After(other.StartedAt)
	}
	return c.ID > other.ID
}

// ScanFilter selects scans for listing. Zero values match everything in that
// dimension; State empty matches both running and completed scans.
type ScanFilter struct {
	SpanID     string
	StationID  string
	Generation uint64
	From       time.Time
	To         time.Time
	State      TraceStatus
	After      *ScanCursor
	Limit      int
}

func (f ScanFilter) matches(trace Trace) bool {
	if f.State != "" && trace.Status != f.State {
		return false
	}
	if f.SpanID != "" && trace.SpanID != f.SpanID {
		return false
	}
	if f.StationID != "" && trace.StationID != f.StationID {
		return false
	}
	if f.Generation != 0 && trace.Generation != f.Generation {
		return false
	}
	if !f.From.IsZero() && trace.StartedAt.Before(f.From) {
		return false
	}
	if !f.To.IsZero() && trace.StartedAt.After(f.To) {
		return false
	}
	return true
}

type ScanPage struct {
	Items      []Trace
	NextCursor ScanCursor
	HasMore    bool
}

// Query returns one page of scans matching the filter. Pages are keyset-based:
// an item sorts into a page only when it sits strictly after filter.After in
// the scan ordering, so scans written while a reader pages forward neither
// shift nor duplicate already-read items.
func (s *SessionService) Query(filter ScanFilter) ScanPage {
	limit := filter.Limit
	if limit < 1 || limit > 200 {
		limit = 50
	}
	s.mu.RLock()
	matched := make([]Trace, 0, len(s.traces))
	for _, trace := range s.traces {
		if filter.matches(trace) {
			matched = append(matched, trace)
		}
	}
	s.mu.RUnlock()
	sort.Slice(matched, func(i, j int) bool {
		return scanKey(matched[i]).before(scanKey(matched[j]))
	})
	page := ScanPage{Items: []Trace{}}
	for _, trace := range matched {
		if filter.After != nil && !filter.After.before(scanKey(trace)) {
			continue
		}
		page.Items = append(page.Items, trace)
		if len(page.Items) > limit {
			break
		}
	}
	if len(page.Items) > limit {
		page.Items = page.Items[:limit]
		page.NextCursor = scanKey(page.Items[limit-1])
		page.HasMore = true
	}
	return page
}
