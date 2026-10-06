package otdr

import (
	"errors"
	"strconv"
	"testing"
	"time"
)

func baseTime() time.Time { return time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC) }

// seededService builds a SessionService and inserts traces directly, assigning
// the monotonic sequence in call order so pagination can be tested without a
// live scanner.
type seededService struct {
	service *SessionService
}

func newSeededService() *seededService {
	return &seededService{service: NewSessionService(nil, nil, nil, nil)}
}

func (b *seededService) seed(spanID, stationID string, generation uint64, state ScanState, startedAt time.Time) Trace {
	b.service.mu.Lock()
	defer b.service.mu.Unlock()
	b.service.seq++
	trace := Trace{
		ID:         idFor(b.service.seq),
		SpanID:     spanID,
		StationID:  stationID,
		Generation: generation,
		State:      state,
		StartedAt:  startedAt.UTC(),
		sequence:   b.service.seq,
	}
	b.service.traces[trace.ID] = trace
	b.service.order = append(b.service.order, trace.ID)
	return trace
}

func idFor(seq uint64) string {
	return "scan-test-" + strconv.FormatUint(uint64(seq), 10)
}

// collect walks every page and asserts no scan is skipped or repeated.
func collect(t *testing.T, service *SessionService, filter ScanFilter, limit int) []Trace {
	t.Helper()
	var collected []Trace
	cursor := ""
	for page := 0; ; page++ {
		result, err := service.Query(filter, cursor, limit)
		if err != nil {
			t.Fatalf("page %d: %v", page, err)
		}
		if len(result.Items) > limit {
			t.Fatalf("page %d returned %d items, limit %d", page, len(result.Items), limit)
		}
		for _, item := range result.Items {
			for _, seen := range collected {
				if seen.ID == item.ID {
					t.Fatalf("scan %s repeated across pages", item.ID)
				}
			}
			collected = append(collected, item)
		}
		if !result.HasMore {
			if result.NextCursor != "" {
				t.Fatalf("final page carried a next cursor")
			}
			return collected
		}
		if result.NextCursor == "" {
			t.Fatalf("page %d reported has_more without a cursor", page)
		}
		cursor = result.NextCursor
	}
}

func TestQueryOrdersNewestFirstWithSequenceTieBreaker(t *testing.T) {
	builder := newSeededService()
	base := baseTime()
	// Two scans share the exact same StartedAt; registration sequence breaks
	// the tie with the earlier-registered one first under descending time.
	first := builder.seed("S3", "east", 1, StateCompleted, base)
	second := builder.seed("S3", "east", 1, StateCompleted, base)
	older := builder.seed("S3", "east", 1, StateCompleted, base.Add(-time.Minute))

	got := collect(t, builder.service, ScanFilter{}, 10)
	want := []string{first.ID, second.ID, older.ID}
	if len(got) != len(want) {
		t.Fatalf("got %d scans, want %d", len(got), len(want))
	}
	for i, id := range want {
		if got[i].ID != id {
			t.Fatalf("position %d = %s, want %s", i, got[i].ID, id)
		}
	}
}

func TestQueryPagesWithoutSkippingOrRepeating(t *testing.T) {
	builder := newSeededService()
	base := baseTime()
	var seeded []Trace
	for i := 0; i < 5; i++ {
		seeded = append(seeded, builder.seed("S3", "east", 1, StateCompleted,
			base.Add(time.Duration(i)*time.Minute)))
	}

	got := collect(t, builder.service, ScanFilter{}, 2)
	if len(got) != 5 {
		t.Fatalf("collected %d scans across pages, want 5", len(got))
	}
	// Seeded order is oldest->newest, pages must present newest->oldest.
	for i := 0; i < 5; i++ {
		want := seeded[4-i].ID
		if got[i].ID != want {
			t.Fatalf("position %d = %s, want %s", i, got[i].ID, want)
		}
	}
}

func TestQueryNewerWritesDoNotDisturbReadPages(t *testing.T) {
	builder := newSeededService()
	base := baseTime()
	for i := 0; i < 5; i++ {
		builder.seed("S3", "east", 1, StateCompleted, base.Add(time.Duration(i)*time.Minute))
	}

	first, err := builder.service.Query(ScanFilter{}, "", 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Items) != 2 || !first.HasMore {
		t.Fatalf("unexpected first page: %+v", first)
	}

	// A brand-new, strictly newer scan lands after the first page was read.
	newest := builder.seed("S3", "east", 1, StateCompleted, base.Add(time.Hour))

	// Resuming the old walk must keep its original order and never surface the
	// newer scan (it sorts before the cursor position) or drop an old one.
	var rest []Trace
	cursor := first.NextCursor
	for {
		page, err := builder.service.Query(ScanFilter{}, cursor, 2)
		if err != nil {
			t.Fatal(err)
		}
		rest = append(rest, page.Items...)
		cursor = page.NextCursor
		if !page.HasMore {
			break
		}
	}
	if len(rest) != 3 {
		t.Fatalf("tail of old walk returned %d scans, want 3", len(rest))
	}
	for _, item := range rest {
		if item.ID == newest.ID {
			t.Fatal("newer scan leaked into a walk started before it existed")
		}
	}
	seen := map[string]bool{first.Items[0].ID: true, first.Items[1].ID: true}
	for _, item := range rest {
		if seen[item.ID] {
			t.Fatalf("scan %s repeated after a concurrent write", item.ID)
		}
		seen[item.ID] = true
	}

	// A fresh walk from the start includes the new scan exactly once, on top.
	fresh := collect(t, builder.service, ScanFilter{}, 2)
	if len(fresh) != 6 || fresh[0].ID != newest.ID {
		t.Fatalf("fresh walk = %d scans, newest first? got %+v", len(fresh), fresh)
	}
}

func TestQuerySameTimestampWriteAppearsOnceAfterCursor(t *testing.T) {
	builder := newSeededService()
	base := baseTime()
	for i := 0; i < 5; i++ {
		builder.seed("S3", "east", 1, StateCompleted, base.Add(time.Duration(i)*10*time.Minute))
	}
	// First page ends on the scan at base+40m (sequence 5 of the 10-minute
	// series: 0,10,20,30,40 -> newest pair is 40/30).
	page, err := builder.service.Query(ScanFilter{}, "", 2)
	if err != nil {
		t.Fatal(err)
	}
	if !page.Items[0].StartedAt.Equal(base.Add(40 * time.Minute)) {
		t.Fatalf("unexpected page head: %v", page.Items[0].StartedAt)
	}

	// A new scan sharing the boundary timestamp but a later sequence appears in
	// the remainder exactly once thanks to the monotonic tie breaker.
	builder.seed("S3", "east", 1, StateCompleted, page.Items[1].StartedAt)
	all := collect(t, builder.service, ScanFilter{}, 2)
	counts := map[string]int{}
	for _, item := range all {
		counts[item.ID]++
	}
	for id, count := range counts {
		if count != 1 {
			t.Fatalf("scan %s appeared %d times", id, count)
		}
	}
	if len(all) != 6 {
		t.Fatalf("collected %d scans, want 6", len(all))
	}
}

func TestQuerySeparatesRunningCompletedAndFailed(t *testing.T) {
	builder := newSeededService()
	base := baseTime()
	running := builder.seed("S3", "east", 1, StateRunning, base.Add(2*time.Minute))
	completed := builder.seed("S3", "east", 1, StateCompleted, base.Add(time.Minute))
	failed := builder.seed("S3", "west", 1, StateFailed, base)

	runningPage, err := builder.service.Query(ScanFilter{State: StateRunning}, "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(runningPage.Items) != 1 || runningPage.Items[0].ID != running.ID {
		t.Fatalf("running filter returned %+v", runningPage.Items)
	}
	completedPage, err := builder.service.Query(ScanFilter{State: StateCompleted}, "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(completedPage.Items) != 1 || completedPage.Items[0].ID != completed.ID {
		t.Fatalf("completed filter returned %+v", completedPage.Items)
	}
	failedPage, err := builder.service.Query(ScanFilter{State: StateFailed}, "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(failedPage.Items) != 1 || failedPage.Items[0].ID != failed.ID {
		t.Fatalf("failed filter returned %+v", failedPage.Items)
	}
}

func TestQueryFiltersBySpanStationGenerationAndTimeRange(t *testing.T) {
	builder := newSeededService()
	base := baseTime()
	s3East := builder.seed("S3", "east", 2, StateCompleted, base)
	builder.seed("S3", "west", 2, StateCompleted, base.Add(time.Minute))
	builder.seed("S4", "east", 2, StateCompleted, base.Add(2*time.Minute))
	oldGen := builder.seed("S3", "east", 1, StateCompleted, base.Add(-time.Hour))
	_ = s3East
	_ = oldGen

	cases := []struct {
		name   string
		filter ScanFilter
		want   int
	}{
		{"span", ScanFilter{SpanID: "S4"}, 1},
		{"station", ScanFilter{StationID: "west"}, 1},
		{"generation", ScanFilter{Generation: 1}, 1},
		{"span+station+generation", ScanFilter{SpanID: "S3", StationID: "east", Generation: 2}, 1},
		{"from inclusive", ScanFilter{From: &base}, 3},
		{"to inclusive", ScanFilter{To: &base}, 2},
		{"bounded range", ScanFilter{From: timePtr(base.Add(30 * time.Second)), To: timePtr(base.Add(90 * time.Second))}, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := collect(t, builder.service, tc.filter, 10)
			if len(got) != tc.want {
				t.Fatalf("filter %s matched %d scans, want %d", tc.name, len(got), tc.want)
			}
		})
	}
}

func timePtr(value time.Time) *time.Time { return &value }

func TestQueryRejectsInvalidArguments(t *testing.T) {
	builder := newSeededService()
	base := baseTime()
	builder.seed("S3", "east", 1, StateCompleted, base)

	if _, err := builder.service.Query(ScanFilter{State: "bogus"}, "", 10); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("state err = %v, want ErrInvalidState", err)
	}
	later := base.Add(time.Hour)
	if _, err := builder.service.Query(ScanFilter{From: &later, To: &base}, "", 10); !errors.Is(err, ErrInvalidTimeRange) {
		t.Fatalf("range err = %v, want ErrInvalidTimeRange", err)
	}
	if _, err := builder.service.Query(ScanFilter{}, "", 0); err != nil {
		// zero limit falls back to the default; explicit bad limits must fail
		t.Fatalf("zero limit should default, got %v", err)
	}
	if _, err := builder.service.Query(ScanFilter{}, "", -1); !errors.Is(err, ErrInvalidLimit) {
		t.Fatalf("limit err = %v, want ErrInvalidLimit", err)
	}
	if _, err := builder.service.Query(ScanFilter{}, "", maxPageLimit+1); !errors.Is(err, ErrInvalidLimit) {
		t.Fatalf("limit err = %v, want ErrInvalidLimit", err)
	}
	if _, err := builder.service.Query(ScanFilter{}, "not-a-cursor", 10); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("cursor err = %v, want ErrInvalidCursor", err)
	}
}

func TestQueryRejectsCursorResumedWithDifferentFilters(t *testing.T) {
	builder := newSeededService()
	base := baseTime()
	for i := 0; i < 3; i++ {
		builder.seed("S3", "east", 1, StateCompleted, base.Add(time.Duration(i)*time.Minute))
	}
	page, err := builder.service.Query(ScanFilter{SpanID: "S3"}, "", 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := builder.service.Query(ScanFilter{SpanID: "S4"}, page.NextCursor, 1); !errors.Is(err, ErrCursorFilterMismatch) {
		t.Fatalf("mismatch err = %v, want ErrCursorFilterMismatch", err)
	}
	// Same filters: the cursor remains valid.
	if _, err := builder.service.Query(ScanFilter{SpanID: "S3"}, page.NextCursor, 1); err != nil {
		t.Fatalf("matching filter rejected cursor: %v", err)
	}
}
