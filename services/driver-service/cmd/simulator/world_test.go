package main

import (
	"context"
	"testing"
	"time"
)

// Runs the whole simulation for a few seconds. Run with: go test -race ./cmd/simulator/
func TestSimulatorSmoke(t *testing.T) {
	cfg := config{
		drivers:      60,
		rate:         30,
		workers:      2,
		queue:        64,
		offerTimeout: 2 * time.Second,
		updateEvery:  200 * time.Millisecond,
		speed:        300,
	}
	w := newWorld(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	w.run(ctx, newHub())
	time.Sleep(6 * time.Second)

	s := w.last.Load()
	if s == nil {
		t.Fatal("no stats were produced")
	}
	if s.Drivers < 50 {
		t.Errorf("drivers online = %d, want about 60", s.Drivers)
	}
	if s.Requests < 100 {
		t.Errorf("requests = %d, want at least 100", s.Requests)
	}
	if s.Updates < 500 {
		t.Errorf("location updates = %d, want at least 500", s.Updates)
	}
	if s.Matched == 0 || s.Accepted == 0 {
		t.Errorf("matched = %d, accepted = %d, want both above 0", s.Matched, s.Accepted)
	}
	if s.Reserved+s.EnRoute+s.InRide+s.Available != s.Drivers {
		t.Errorf("driver states don't add up: %+v", s)
	}
	t.Logf("requests=%d updates=%d matched=%d accepted=%d declined=%d timeouts=%d cancelled=%d no_drivers=%d completed=%d",
		s.Requests, s.Updates, s.Matched, s.Accepted, s.Declined, s.Timeouts, s.Cancelled, s.NoDrivers, s.Completed)

	cancel()
	w.close()
}