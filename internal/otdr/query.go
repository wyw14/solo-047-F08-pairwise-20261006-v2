package otdr

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

const (
	// defaultPageLimit is used when the caller does not request a page size.
	defaultPageLimit = 50
	// maxPageLimit bounds the work a single query can do.
	maxPageLimit = 200
)

var (
	// ErrInvalidCursor is returned when a continuation cursor cannot be decoded.
	ErrInvalidCursor = errors.New("invalid scan cursor")
	// ErrCursorFilterMismatch is returned when a cursor is resumed with
	// filters different from the ones it was created with.
	ErrCursorFilterMismatch = errors.New("cursor does not match the query filters")
	// ErrInvalidTimeRange is returned when from is later than to.
	ErrInvalidTimeRange = errors.New("invalid scan time range: from is after to")
	// ErrInvalidState is returned for an unknown state filter.
	ErrInvalidState = errors.New("invalid scan state")
	// ErrInvalidLimit is returned for page sizes outside [1, maxPageLimit].
	ErrInvalidLimit = errors.New("invalid scan page limit")
)

// ScanFilter narrows a scan listing. Zero values mean "no constraint" except
// Generation, where 0 means unconstrained. The time range applies to
// StartedAt and is inclusive on both ends.
type ScanFilter struct {
	SpanID     string
	StationID  string
	Generation uint64
	From       *time.Time
	To         *time.Time
	State      ScanState
}

// ScanPage is one keyset page. When HasMore is true, NextCursor must be passed
// back as the cursor of the next request.
type ScanPage struct {
	Items      []Trace `json:"items"`
	NextCursor string  `json:"next_cursor,omitempty"`
	HasMore    bool    `json:"has_more"`
}

// Query returns one stable, filterable keyset page of scans ordered by
// StartedAt descending, breaking ties on the monotonic registration sequence.
// Newly written scans cannot reshuffle pages already handed out: a scan only
// ever appears after the cursor position it sorts after.
func (s *SessionService) Query(filter ScanFilter, rawCursor string, limit int) (ScanPage, error) {
	if err := filter.Validate(); err != nil {
		return ScanPage{}, err
	}
	if limit == 0 {
		limit = defaultPageLimit
	}
	if limit < 1 || limit > maxPageLimit {
		return ScanPage{}, fmt.Errorf("%w: must be between 1 and %d", ErrInvalidLimit, maxPageLimit)
	}
	cursor, err := decodeCursor(rawCursor)
	if err != nil {
		return ScanPage{}, err
	}
	if rawCursor != "" && !cursor.matches(filter) {
		return ScanPage{}, ErrCursorFilterMismatch
	}

	matched := make([]Trace, 0)
	for _, trace := range s.all() {
		if !filter.accepts(trace) {
			continue
		}
		if cursor != nil && !after(trace, *cursor) {
			continue
		}
		matched = append(matched, trace)
	}
	// s.all() is already in registration (sequence ascending) order; a stable
	// sort by StartedAt descending keeps that sequence as the tie breaker.
	sort.SliceStable(matched, func(i, j int) bool {
		return matched[i].StartedAt.After(matched[j].StartedAt)
	})

	page := ScanPage{Items: []Trace{}}
	if len(matched) > limit {
		page.HasMore = true
		last := matched[limit-1]
		page.Items = append(page.Items, matched[:limit]...)
		next, err := encodeCursor(last, filter)
		if err != nil {
			return ScanPage{}, err
		}
		page.NextCursor = next
	} else {
		page.Items = append(page.Items, matched...)
	}
	return page, nil
}

// Validate checks the filter independent of any cursor.
func (f ScanFilter) Validate() error {
	if f.State != "" && f.State != StateRunning && f.State != StateCompleted && f.State != StateFailed {
		return fmt.Errorf("%w: %q", ErrInvalidState, f.State)
	}
	if f.From != nil && f.To != nil && f.From.After(*f.To) {
		return ErrInvalidTimeRange
	}
	return nil
}

func (f ScanFilter) accepts(trace Trace) bool {
	if f.SpanID != "" && trace.SpanID != f.SpanID {
		return false
	}
	if f.StationID != "" && trace.StationID != f.StationID {
		return false
	}
	if f.Generation != 0 && trace.Generation != f.Generation {
		return false
	}
	if f.State != "" && trace.State != f.State {
		return false
	}
	started := trace.StartedAt
	if f.From != nil && started.Before(*f.From) {
		return false
	}
	if f.To != nil && started.After(*f.To) {
		return false
	}
	return true
}

// after reports whether trace sorts strictly after the cursor position in
// listing order (StartedAt descending, sequence ascending for ties): it must
// be older than the cursor row, or share its timestamp with a higher sequence.
func after(trace Trace, c cursorPayload) bool {
	if !trace.StartedAt.Equal(c.StartedAt) {
		return trace.StartedAt.Before(c.StartedAt)
	}
	return trace.sequence > c.Seq
}

type cursorPayload struct {
	StartedAt  time.Time  `json:"started_at"`
	Seq        uint64     `json:"seq"`
	SpanID     string     `json:"span_id,omitempty"`
	StationID  string     `json:"station_id,omitempty"`
	Generation uint64     `json:"generation,omitempty"`
	From       *time.Time `json:"from,omitempty"`
	To         *time.Time `json:"to,omitempty"`
	State      string     `json:"state,omitempty"`
}

func (c cursorPayload) matches(f ScanFilter) bool {
	return c.SpanID == f.SpanID &&
		c.StationID == f.StationID &&
		c.Generation == f.Generation &&
		c.State == string(f.State) &&
		compareTimePtr(c.From, f.From) &&
		compareTimePtr(c.To, f.To)
}

func compareTimePtr(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Equal(*b)
}

func encodeCursor(trace Trace, filter ScanFilter) (string, error) {
	payload := cursorPayload{
		StartedAt: trace.StartedAt.UTC(), Seq: trace.sequence,
		SpanID: filter.SpanID, StationID: filter.StationID, Generation: filter.Generation,
		State: string(filter.State),
	}
	if filter.From != nil {
		from := filter.From.UTC()
		payload.From = &from
	}
	if filter.To != nil {
		to := filter.To.UTC()
		payload.To = &to
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("encode scan cursor: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func decodeCursor(raw string) (*cursorPayload, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrInvalidCursor, "undecodable")
	}
	var payload cursorPayload
	if err := json.Unmarshal(decoded, &payload); err != nil {
		return nil, fmt.Errorf("%w: %s", ErrInvalidCursor, "malformed")
	}
	if payload.Seq == 0 || payload.StartedAt.IsZero() {
		return nil, fmt.Errorf("%w: %s", ErrInvalidCursor, "missing position")
	}
	payload.StartedAt = payload.StartedAt.UTC()
	return &payload, nil
}
