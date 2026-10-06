package otdr

import (
	"context"
	"testing"
	"time"

	"github.com/wyw14/cry-103/internal/landing"
	"github.com/wyw14/cry-103/internal/platform"
	"github.com/wyw14/cry-103/internal/topology"
)

func newSessionService(t *testing.T, now *time.Time) *SessionService {
	t.Helper()
	registry := topology.NewRegistry()
	if _, err := registry.Register("S3", topology.PathPrimary, *now); err != nil {
		t.Fatal(err)
	}
	return NewSessionService(registry, landing.NewScanner(registry, 0), platform.UUIDGenerator{}, func() time.Time {
		return *now
	})
}

func TestQueryOrdersAndPaginatesStably(t *testing.T) {
	now := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	service := newSessionService(t, &now)

	// Two scans share the same StartedAt; the ID tiebreak must keep the
	// ordering total and deterministic.
	first, err := service.Run(context.Background(), "S3", "east", 126, 2)
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.Run(context.Background(), "S3", "west", 126, 2)
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Minute)
	third, err := service.Run(context.Background(), "S3", "east", 126, 2)
	if err != nil {
		t.Fatal(err)
	}

	page := service.Query(ScanFilter{State: StatusCompleted, Limit: 10})
	if len(page.Items) != 3 || page.HasMore {
		t.Fatalf("unexpected page: %+v", page)
	}
	if page.Items[0].ID != third.ID {
		t.Fatalf("newest scan must sort first, got %s", page.Items[0].ID)
	}
	if !page.Items[1].StartedAt.Equal(page.Items[2].StartedAt) {
		t.Fatalf("expected shared timestamps at the tail: %+v", page.Items)
	}
	if page.Items[1].ID <= page.Items[2].ID {
		t.Fatalf("identical timestamps must fall back to descending IDs: %+v", page.Items)
	}
	again := service.Query(ScanFilter{State: StatusCompleted, Limit: 10})
	for index := range page.Items {
		if again.Items[index].ID != page.Items[index].ID {
			t.Fatalf("ordering is not deterministic: %+v vs %+v", page.Items, again.Items)
		}
	}

	// Walking one item at a time must visit every scan exactly once.
	seen := map[string]bool{}
	var after *ScanCursor
	for {
		page := service.Query(ScanFilter{State: StatusCompleted, Limit: 1, After: after})
		for _, item := range page.Items {
			if seen[item.ID] {
				t.Fatalf("scan %s returned twice", item.ID)
			}
			seen[item.ID] = true
		}
		if !page.HasMore {
			break
		}
		cursor := page.NextCursor
		after = &cursor
	}
	for _, id := range []string{first.ID, second.ID, third.ID} {
		if !seen[id] {
			t.Fatalf("scan %s was skipped; saw %v", id, seen)
		}
	}
}

func TestQueryFiltersByDimensions(t *testing.T) {
	now := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	service := newSessionService(t, &now)

	east, err := service.Run(context.Background(), "S3", "east", 126, 2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Run(context.Background(), "S3", "west", 126, 2); err != nil {
		t.Fatal(err)
	}

	page := service.Query(ScanFilter{State: StatusCompleted, StationID: "east", Limit: 10})
	if len(page.Items) != 1 || page.Items[0].ID != east.ID {
		t.Fatalf("station filter=%+v, want only %s", page.Items, east.ID)
	}
	if page := service.Query(ScanFilter{State: StatusCompleted, Generation: 2, Limit: 10}); len(page.Items) != 0 {
		t.Fatalf("generation filter matched %+v, want none", page.Items)
	}
	if page := service.Query(ScanFilter{State: StatusCompleted, From: now.Add(time.Minute), Limit: 10}); len(page.Items) != 0 {
		t.Fatalf("future from matched %+v, want none", page.Items)
	}
	if page := service.Query(ScanFilter{State: StatusRunning, Limit: 10}); len(page.Items) != 0 {
		t.Fatalf("no scan is running, got %+v", page.Items)
	}
}

func TestFailedScanDoesNotStayListed(t *testing.T) {
	now := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	service := newSessionService(t, &now)

	if _, err := service.Run(context.Background(), "S3", "east", 126, 0); err == nil {
		t.Fatal("a scan with zero chunks must fail")
	}
	for _, state := range []TraceStatus{StatusRunning, StatusCompleted} {
		if page := service.Query(ScanFilter{State: state, Limit: 10}); len(page.Items) != 0 {
			t.Fatalf("failed scan must not stay listed as %s: %+v", state, page.Items)
		}
	}
}
