package matcher

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"ride-sharing/services/driver-service/internal/index"
)

// n drivers within about 550 m of (9.0, 38.7).
func newTestIndex(n int) *index.Index {
	idx := index.New(16, 6)
	for i := 0; i < n; i++ {
		idx.Upsert(fmt.Sprintf("d%d", i), "suv", 9.0+float64(i)*0.00005, 38.7)
	}
	return idx
}

// A matcher with NO workers started, so we control exactly when jobs are processed.
func newManualMatcher(idx *index.Index, queueSize int) *Matcher {
	return &Matcher{
		idx:   idx,
		cfg:   Config{SearchLimit: 5, MaxRadiusMeters: 5000},
		queue: make(chan job, queueSize),
	}
}

var pickup = Request{PackageSlug: "suv", Lat: 9.0, Lng: 38.7}

func TestMatchReturnsNearestAndReserves(t *testing.T) {
	idx := index.New(8, 6)
	idx.Upsert("far", "suv", 9.002, 38.7)
	idx.Upsert("near", "suv", 9.001, 38.7)

	p := New(idx, Config{Workers: 2, QueueSize: 8})
	defer p.Close()

	res, err := p.Match(context.Background(), pickup)
	if err != nil || !res.Matched || res.DriverID != "near" {
		t.Fatalf("res=%+v err=%v, want driver near", res, err)
	}
	if d, _ := idx.Get("near"); d.State != index.StateReserved {
		t.Fatalf("near state = %v, want reserved", d.State)
	}
	if d, _ := idx.Get("far"); d.State != index.StateAvailable {
		t.Fatalf("far state = %v, want available", d.State)
	}
}

func TestNoDriverIsNotAnError(t *testing.T) {
	p := New(index.New(8, 6), Config{Workers: 2, QueueSize: 8})
	defer p.Close()

	res, err := p.Match(context.Background(), pickup)
	if err != nil || res.Matched {
		t.Fatalf("res=%+v err=%v, want no match and no error", res, err)
	}
}

func TestQueueFullRejectsImmediately(t *testing.T) {
	p := newManualMatcher(newTestIndex(5), 2)

	for i := 0; i < 2; i++ {
		if _, err := p.Submit(context.Background(), pickup); err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
	}
	if _, err := p.Submit(context.Background(), pickup); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("err = %v, want ErrQueueFull", err)
	}
	if p.Stats().Rejected != 1 {
		t.Fatalf("rejected = %d, want 1", p.Stats().Rejected)
	}
	p.Close()
}

func TestCloseDrainsQueueThenRejects(t *testing.T) {
	p := New(newTestIndex(100), Config{Workers: 2, QueueSize: 100})

	chans := make([]<-chan Result, 0, 50)
	for i := 0; i < 50; i++ {
		ch, err := p.Submit(context.Background(), pickup)
		if err != nil {
			t.Fatal(err)
		}
		chans = append(chans, ch)
	}
	p.Close() // waits until all 50 are processed

	for i, ch := range chans {
		select {
		case <-ch:
		default:
			t.Fatalf("request %d was dropped on close", i)
		}
	}
	if _, err := p.Submit(context.Background(), pickup); !errors.Is(err, ErrClosed) {
		t.Fatalf("err = %v, want ErrClosed", err)
	}
}

func TestCancelledWhileQueuedIsSkipped(t *testing.T) {
	idx := newTestIndex(10)
	p := newManualMatcher(idx, 4)

	ctx, cancel := context.WithCancel(context.Background())
	ch, err := p.Submit(ctx, pickup)
	if err != nil {
		t.Fatal(err)
	}
	cancel() // the rider gives up while still queued

	p.wg.Add(1)
	go p.worker()
	p.Close()

	res := <-ch
	if !errors.Is(res.Err, context.Canceled) || res.Matched {
		t.Fatalf("res = %+v, want a cancelled result", res)
	}
	for i := 0; i < 10; i++ {
		if d, _ := idx.Get(fmt.Sprintf("d%d", i)); d.State != index.StateAvailable {
			t.Fatalf("driver d%d was reserved for a cancelled request", i)
		}
	}
	if p.Stats().Cancelled != 1 {
		t.Fatalf("cancelled = %d, want 1", p.Stats().Cancelled)
	}
}

func TestMatchTimeoutDoesNotLeakDrivers(t *testing.T) {
	idx := newTestIndex(10)
	p := newManualMatcher(idx, 4) // no workers yet, so the request just waits

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := p.Match(ctx, pickup); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want deadline exceeded", err)
	}

	p.wg.Add(1)
	go p.worker()
	p.Close()

	for i := 0; i < 10; i++ {
		if d, _ := idx.Get(fmt.Sprintf("d%d", i)); d.State != index.StateAvailable {
			t.Fatalf("driver d%d stayed reserved after the caller timed out", i)
		}
	}
}

// 10,000 requests through the matcher for 100 drivers: exactly 100 matches,
// every driver matched exactly once.
func TestMatcherNoDoubleBooking(t *testing.T) {
	const drivers, requests = 100, 10000

	idx := newTestIndex(drivers)
	p := New(idx, Config{Workers: 16, QueueSize: requests})

	chans := make([]<-chan Result, 0, requests)
	for i := 0; i < requests; i++ {
		ch, err := p.Submit(context.Background(), pickup)
		if err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
		chans = append(chans, ch)
	}

	wins := map[string]int{}
	for _, ch := range chans {
		if res := <-ch; res.Matched {
			wins[res.DriverID]++
		}
	}
	p.Close()

	if len(wins) != drivers {
		t.Fatalf("%d distinct drivers matched, want %d", len(wins), drivers)
	}
	for id, n := range wins {
		if n != 1 {
			t.Fatalf("driver %s matched %d times (double booking)", id, n)
		}
	}
	st := p.Stats()
	if st.Matched != drivers || st.NoDriver != requests-drivers {
		t.Fatalf("stats = %+v, want matched=%d no_driver=%d", st, drivers, requests-drivers)
	}
}

// Submitting while Close runs must never panic ("send on closed channel").
func TestSubmitRacesWithClose(t *testing.T) {
	p := New(newTestIndex(10), Config{Workers: 4, QueueSize: 16})

	var wg sync.WaitGroup
	for g := 0; g < 50; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				_, err := p.Submit(context.Background(), pickup)
				if err != nil && !errors.Is(err, ErrQueueFull) && !errors.Is(err, ErrClosed) {
					t.Errorf("unexpected error: %v", err)
					return
				}
			}
		}()
	}
	time.Sleep(5 * time.Millisecond)
	p.Close()
	wg.Wait()
}