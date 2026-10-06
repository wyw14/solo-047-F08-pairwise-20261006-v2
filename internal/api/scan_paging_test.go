package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func startScanForTest(t *testing.T, server *Server, stationID string) otdrTraceEnvelope {
	t.Helper()
	body, _ := json.Marshal(ScanRequest{StationID: stationID, LengthKM: 126, Chunks: 2})
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/spans/S3/otdr-scans", bytes.NewReader(body)))
	if recorder.Code != http.StatusCreated {
		t.Fatalf("start scan status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var trace otdrTraceEnvelope
	if err := json.Unmarshal(recorder.Body.Bytes(), &trace); err != nil {
		t.Fatal(err)
	}
	return trace
}

// otdrTraceEnvelope mirrors the subset of otdr.Trace fields this test asserts.
type otdrTraceEnvelope struct {
	ID        string `json:"id"`
	SpanID    string `json:"span_id"`
	StationID string `json:"station_id"`
	State     string `json:"state"`
}

type scanPageEnvelope struct {
	Items      []otdrTraceEnvelope `json:"items"`
	NextCursor string              `json:"next_cursor"`
	HasMore    bool                `json:"has_more"`
}

func listScansForTest(t *testing.T, server *Server, target string) scanPageEnvelope {
	t.Helper()
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/otdr-scans?"+target, nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("list scans (%s) status=%d body=%s", target, recorder.Code, recorder.Body.String())
	}
	var page scanPageEnvelope
	if err := json.Unmarshal(recorder.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	return page
}

func TestListScansCursorPaginationCoversEveryScanOnce(t *testing.T) {
	server := testServer(t)
	started := map[string]bool{}
	for i := 0; i < 5; i++ {
		trace := startScanForTest(t, server, "east")
		if trace.State != "completed" {
			t.Fatalf("scan %s state = %q, want completed", trace.ID, trace.State)
		}
		started[trace.ID] = true
	}

	seen := map[string]bool{}
	cursor := ""
	pages := 0
	for {
		target := "limit=2"
		if cursor != "" {
			target += "&cursor=" + cursor
		}
		page := listScansForTest(t, server, target)
		pages++
		if len(page.Items) > 2 {
			t.Fatalf("page carried %d items, limit 2", len(page.Items))
		}
		for _, item := range page.Items {
			if !started[item.ID] {
				t.Fatalf("unexpected scan %s in page", item.ID)
			}
			if seen[item.ID] {
				t.Fatalf("scan %s repeated across pages", item.ID)
			}
			seen[item.ID] = true
		}
		if !page.HasMore {
			break
		}
		cursor = page.NextCursor
	}
	if len(seen) != 5 || pages != 3 {
		t.Fatalf("walk covered %d scans in %d pages, want 5 in 3", len(seen), pages)
	}
}

func TestListScansFilters(t *testing.T) {
	server := testServer(t)
	east := startScanForTest(t, server, "east")
	west := startScanForTest(t, server, "west")

	if page := listScansForTest(t, server, "station_id=east"); len(page.Items) != 1 || page.Items[0].ID != east.ID {
		t.Fatalf("station filter returned %+v", page.Items)
	}
	if page := listScansForTest(t, server, "station_id=west"); len(page.Items) != 1 || page.Items[0].ID != west.ID {
		t.Fatalf("station filter returned %+v", page.Items)
	}
	if page := listScansForTest(t, server, "span_id=S3&generation=1"); len(page.Items) != 2 {
		t.Fatalf("span+generation filter returned %d scans, want 2", len(page.Items))
	}
	if page := listScansForTest(t, server, "span_id=S4"); len(page.Items) != 0 {
		t.Fatalf("unknown span returned %d scans", len(page.Items))
	}
	if page := listScansForTest(t, server, "state=completed"); len(page.Items) != 2 {
		t.Fatalf("completed state returned %d scans, want 2", len(page.Items))
	}
	if page := listScansForTest(t, server, "state=running"); len(page.Items) != 0 {
		t.Fatalf("running state returned %d scans, want 0", len(page.Items))
	}
}

func TestListScansRejectsBadQueryParameters(t *testing.T) {
	server := testServer(t)
	startScanForTest(t, server, "east")

	cases := []string{
		"generation=0",
		"generation=abc",
		"from=not-a-time",
		"to=2026-13-99T00:00:00Z",
		"limit=0",
		"limit=-3",
		"limit=nope",
		"state=queued",
		"cursor=abc",
	}
	for _, target := range cases {
		recorder := httptest.NewRecorder()
		server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/otdr-scans?"+target, nil))
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("target %q: status=%d, want 400", target, recorder.Code)
		}
	}
}

func TestListScansCursorMustMatchFilters(t *testing.T) {
	server := testServer(t)
	startScanForTest(t, server, "east")
	page := listScansForTest(t, server, "station_id=east")
	_ = page
	// Obtain a cursor by forcing a second scan and a bounded page.
	startScanForTest(t, server, "east")
	paged := listScansForTest(t, server, "station_id=east&limit=1")
	if !paged.HasMore || paged.NextCursor == "" {
		t.Fatalf("expected a continuation cursor, got %+v", paged)
	}
	recorder := httptest.NewRecorder()
	target := fmt.Sprintf("station_id=west&limit=1&cursor=%s", paged.NextCursor)
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/otdr-scans?"+target, nil))
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("filter-mismatched cursor status=%d, want 400", recorder.Code)
	}
}
