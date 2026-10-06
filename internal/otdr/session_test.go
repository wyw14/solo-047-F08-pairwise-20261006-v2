package otdr

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/wyw14/cry-103/internal/landing"
	"github.com/wyw14/cry-103/internal/platform"
	"github.com/wyw14/cry-103/internal/topology"
)

func newRunService(delay time.Duration, now time.Time) (*SessionService, *topology.Registry) {
	registry := topology.NewRegistry()
	if _, err := registry.Register("S3", topology.PathPrimary, now); err != nil {
		panic(err)
	}
	clock := func() time.Time { return now }
	service := NewSessionService(registry, landing.NewScanner(registry, delay), platform.UUIDGenerator{}, clock)
	return service, registry
}

func TestRunRegistersRunningBeforeCompletion(t *testing.T) {
	base := baseTime()
	service, _ := newRunService(200*time.Millisecond, base)

	done := make(chan Trace)
	errs := make(chan error, 1)
	go func() {
		trace, err := service.Run(context.Background(), "S3", "east", 126, 2)
		if err != nil {
			errs <- err
			return
		}
		done <- trace
	}()

	// The scan must be listable as running while chunks are still collecting.
	deadline := time.Now().Add(time.Second)
	for {
		page, err := service.Query(ScanFilter{State: StateRunning}, "", 10)
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Items) == 1 {
			running := page.Items[0]
			if running.SpanID != "S3" || running.StationID != "east" || running.Generation != 1 {
				t.Fatalf("running trace missing scan metadata: %+v", running)
			}
			if running.CompletedAt != nil {
				t.Fatal("running scan must not carry a completion timestamp")
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("scan never became visible as running")
		}
		time.Sleep(5 * time.Millisecond)
	}

	var completed Trace
	select {
	case completed = <-done:
	case err := <-errs:
		t.Fatal(err)
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not finish")
	}
	if completed.State != StateCompleted || completed.CompletedAt == nil {
		t.Fatalf("completed trace wrong state: %+v", completed)
	}
	page, err := service.Query(ScanFilter{State: StateCompleted}, "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || page.Items[0].ID != completed.ID {
		t.Fatalf("completed scan not listed: %+v", page.Items)
	}
}

func TestRunMarksFailedAndKeepsRecord(t *testing.T) {
	base := baseTime()
	// Positive delay so wait() actually selects on the cancelled context;
	// wait() returns immediately when delay is zero without observing ctx.
	service, _ := newRunService(200*time.Millisecond, base)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := service.Run(ctx, "S3", "east", 126, 2); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run err = %v, want context.Canceled", err)
	}

	page, err := service.Query(ScanFilter{State: StateFailed}, "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 {
		t.Fatalf("failed listing = %d scans, want 1", len(page.Items))
	}
	failed := page.Items[0]
	if failed.Error == "" || failed.CompletedAt != nil {
		t.Fatalf("failed trace wrong shape: %+v", failed)
	}
	got, exists := service.Get(failed.ID)
	if !exists || got.State != StateFailed {
		t.Fatalf("failed scan not retrievable via Get: exists=%v trace=%+v", exists, got)
	}
}

func TestRunRejectsUnknownSpanWithoutRegistering(t *testing.T) {
	base := baseTime()
	service, _ := newRunService(0, base)
	if _, err := service.Run(context.Background(), "MISSING", "east", 126, 1); err == nil {
		t.Fatal("expected error for unknown span")
	}
	for _, state := range []ScanState{StateRunning, StateCompleted, StateFailed} {
		page, err := service.Query(ScanFilter{State: state}, "", 10)
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Items) != 0 {
			t.Fatalf("state %s leaked a trace for unknown span: %+v", state, page.Items)
		}
	}
}
