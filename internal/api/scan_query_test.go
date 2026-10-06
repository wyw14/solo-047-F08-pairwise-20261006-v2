package api

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wyw14/cry-103/internal/otdr"
	"github.com/wyw14/cry-103/internal/topology"
)

type mutableClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *mutableClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *mutableClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func runScan(t *testing.T, server *Server, spanID, stationID string) otdr.Trace {
	t.Helper()
	body, _ := json.Marshal(ScanRequest{StationID: stationID, LengthKM: 126, Chunks: 2})
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/spans/"+spanID+"/otdr-scans", bytes.NewReader(body)))
	if recorder.Code != http.StatusCreated {
		t.Fatalf("scan status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var trace otdr.Trace
	if err := json.Unmarshal(recorder.Body.Bytes(), &trace); err != nil {
		t.Fatal(err)
	}
	return trace
}

func listScans(t *testing.T, server *Server, rawQuery string) ScanPageResponse {
	t.Helper()
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/otdr-scans?"+rawQuery, nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("list status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var page ScanPageResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	return page
}

func scanIDs(page ScanPageResponse) []string {
	ids := make([]string, 0, len(page.Items))
	for _, trace := range page.Items {
		ids = append(ids, trace.ID)
	}
	return ids
}

func equalIDs(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for index := range a {
		if a[index] != b[index] {
			return false
		}
	}
	return true
}

func TestListScansFiltersPaginationAndStability(t *testing.T) {
	clock := &mutableClock{now: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)}
	server := newTestServer(t, clock, 0)

	first := runScan(t, server, "S3", "east") // T+0m, generation 1
	clock.Advance(time.Minute)
	second := runScan(t, server, "S3", "west") // T+1m, generation 1
	clock.Advance(time.Minute)
	if _, err := server.services.Topology.Switch("S3", topology.PathProtection, clock.Now()); err != nil {
		t.Fatal(err)
	}
	third := runScan(t, server, "S3", "east") // T+2m, generation 2
	clock.Advance(time.Minute)
	fourth := runScan(t, server, "S4", "east") // T+3m, generation 1

	cases := []struct {
		name  string
		query string
		want  []string
	}{
		{"by span", "span_id=S4", []string{fourth.ID}},
		{"by station", "station_id=west", []string{second.ID}},
		{"by generation", "generation=2", []string{third.ID}},
		{"by span and generation", "span_id=S3&generation=1", []string{second.ID, first.ID}},
		{"by span and station", "span_id=S3&station_id=east", []string{third.ID, first.ID}},
		{
			"by time range",
			fmt.Sprintf("from=%s&to=%s",
				clock.now.Add(-2*time.Minute).UTC().Format(time.RFC3339),
				clock.now.Add(-time.Minute).UTC().Format(time.RFC3339)),
			[]string{third.ID, second.ID},
		},
		{"no match", "span_id=unknown", nil},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			page := listScans(t, server, testCase.query)
			if page.State != otdr.StatusCompleted {
				t.Fatalf("state=%q, want completed", page.State)
			}
			if !equalIDs(scanIDs(page), testCase.want) {
				t.Fatalf("ids=%v, want %v", scanIDs(page), testCase.want)
			}
			if page.NextCursor != "" {
				t.Fatalf("unexpected next cursor for single page: %q", page.NextCursor)
			}
		})
	}

	// Walk the full list two at a time: newest first, no duplicates, no gaps.
	var collected []string
	cursor := ""
	for {
		query := "limit=2"
		if cursor != "" {
			query += "&cursor=" + cursor
		}
		page := listScans(t, server, query)
		if len(page.Items) > 2 {
			t.Fatalf("page returned %d items over limit 2", len(page.Items))
		}
		collected = append(collected, scanIDs(page)...)
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	want := []string{fourth.ID, third.ID, second.ID, first.ID}
	if !equalIDs(collected, want) {
		t.Fatalf("walked ids=%v, want %v", collected, want)
	}

	// A scan written after page one must not shift, skip, or duplicate items
	// on the pages a reader has already started from.
	firstPage := listScans(t, server, "limit=2")
	if !equalIDs(scanIDs(firstPage), want[:2]) {
		t.Fatalf("first page=%v, want %v", scanIDs(firstPage), want[:2])
	}
	clock.Advance(time.Minute)
	fifth := runScan(t, server, "S3", "east")
	continuation := listScans(t, server, "limit=2&cursor="+firstPage.NextCursor)
	if !equalIDs(scanIDs(continuation), want[2:]) {
		t.Fatalf("continuation=%v, want %v (new scan %s must not disturb read pages)", scanIDs(continuation), want[2:], fifth.ID)
	}
	if continuation.NextCursor != "" {
		t.Fatalf("unexpected next cursor after final page: %q", continuation.NextCursor)
	}
}

func TestListScansSeparatesRunningFromCompleted(t *testing.T) {
	clock := &mutableClock{now: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)}
	server := newTestServer(t, clock, 75*time.Millisecond)

	type scanResult struct {
		code int
		body []byte
	}
	done := make(chan scanResult, 1)
	go func() {
		body, _ := json.Marshal(ScanRequest{StationID: "east", LengthKM: 126, Chunks: 4})
		recorder := httptest.NewRecorder()
		server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/spans/S3/otdr-scans", bytes.NewReader(body)))
		done <- scanResult{code: recorder.Code, body: recorder.Body.Bytes()}
	}()

	var running ScanPageResponse
	deadline := time.Now().Add(2 * time.Second)
	for {
		running = listScans(t, server, "state=running")
		if len(running.Items) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("scan never appeared in the running list")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if running.State != otdr.StatusRunning || running.Items[0].Status != otdr.StatusRunning {
		t.Fatalf("unexpected running page: %+v", running)
	}
	if page := listScans(t, server, "state=completed"); len(page.Items) != 0 {
		t.Fatalf("running scan leaked into completed list: %v", scanIDs(page))
	}

	result := <-done
	if result.code != http.StatusCreated {
		t.Fatalf("scan status=%d body=%s", result.code, string(result.body))
	}
	completed := listScans(t, server, "state=completed")
	if !equalIDs(scanIDs(completed), scanIDs(running)) {
		t.Fatalf("completed ids=%v, want the previously running %v", scanIDs(completed), scanIDs(running))
	}
	if completed.Items[0].Status != otdr.StatusCompleted {
		t.Fatalf("unexpected completed status: %+v", completed.Items[0])
	}
	if page := listScans(t, server, "state=running"); len(page.Items) != 0 {
		t.Fatalf("completed scan leaked into running list: %v", scanIDs(page))
	}
	// The default view is the completed list.
	if page := listScans(t, server, ""); len(page.Items) != 1 || page.State != otdr.StatusCompleted {
		t.Fatalf("default list=%+v, want one completed scan", page)
	}
}

func TestListScansRejectsInvalidParams(t *testing.T) {
	server := newTestServer(t, &mutableClock{now: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)}, 0)
	cases := []string{
		"cursor=abc",
		"cursor=" + strings.Repeat("a", 5),
		"state=paused",
		"generation=abc",
		"from=not-a-time",
		"to=2026-13-01T00:00:00Z",
		"from=2026-10-02T00:00:00Z&to=2026-10-01T00:00:00Z",
		"limit=-1",
	}
	for _, raw := range cases {
		recorder := httptest.NewRecorder()
		server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/otdr-scans?"+raw, nil))
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("query %q: status=%d body=%s, want 400", raw, recorder.Code, recorder.Body.String())
		}
	}
}

func TestScanCursorCodec(t *testing.T) {
	cursor := otdr.ScanCursor{
		StartedAt: time.Date(2026, 10, 1, 12, 30, 45, 123456789, time.UTC),
		ID:        "scan-00000000-0000-0000-0000-000000000000",
	}
	decoded, err := decodeScanCursor(encodeScanCursor(cursor))
	if err != nil {
		t.Fatal(err)
	}
	if !decoded.StartedAt.Equal(cursor.StartedAt) || decoded.ID != cursor.ID {
		t.Fatalf("round trip=%+v, want %+v", decoded, cursor)
	}
	for _, raw := range []string{
		"abc",
		"!!!",
		base64.RawURLEncoding.EncodeToString([]byte("v2|1700000000000000000|scan-1")),
		base64.RawURLEncoding.EncodeToString([]byte("v1|not-a-number|scan-1")),
		base64.RawURLEncoding.EncodeToString([]byte("v1|1700000000000000000|")),
	} {
		if _, err := decodeScanCursor(raw); err == nil {
			t.Fatalf("expected error decoding %q", raw)
		}
	}
}
