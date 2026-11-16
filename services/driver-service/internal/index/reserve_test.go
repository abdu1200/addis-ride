package index

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

func TestStateTransitions(t *testing.T) {
	idx := New(8, 6)
	idx.Upsert("d1", "suv", 9.0, 38.7)

	if idx.Release("d1") {
		t.Fatal("an available driver can't be released")
	}
	if !idx.Reserve("d1") {
		t.Fatal("available -> reserved should work")
	}
	if idx.Reserve("d1") {
		t.Fatal("a reserved driver can't be reserved again")
	}
	if idx.Complete("d1") {
		t.Fatal("a reserved driver can't be completed")
	}
	if !idx.Assign("d1") {
		t.Fatal("reserved -> assigned should work")
	}
	if idx.Release("d1") {
		t.Fatal("an assigned driver can't be released")
	}
	if got, _ := idx.Get("d1"); got.State != StateAssigned {
		t.Fatalf("state = %v, want assigned", got.State)
	}
	if !idx.Complete("d1") {
		t.Fatal("assigned -> available should work")
	}
	if got, _ := idx.Get("d1"); got.State != StateAvailable {
		t.Fatalf("state = %v, want available", got.State)
	}
	if idx.Reserve("nobody") {
		t.Fatal("unknown driver must not be reservable")
	}
}

// 500 goroutines x 20 attempts = 10,000 attempts on ONE driver. Exactly one wins.
func TestReserveOnlyOneWinner(t *testing.T) {
	const workers, attempts = 500, 20
	idx := New(8, 6)
	idx.Upsert("d1", "suv", 9.0, 38.7)

	var wins atomic.Int32
	start := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < attempts; i++ {
				if idx.Reserve("d1") {
					wins.Add(1)
				}
			}
		}()
	}
	close(start)
	wg.Wait()

	if wins.Load() != 1 {
		t.Fatalf("%d goroutines won the same driver, want exactly 1", wins.Load())
	}
}

// Many reserve/release cycles: at no moment may two goroutines hold the driver.
func TestReserveReleaseMutualExclusion(t *testing.T) {
	const workers, cycles = 16, 2000
	idx := New(8, 6)
	idx.Upsert("d1", "suv", 9.0, 38.7)

	var holders atomic.Int32
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < cycles; i++ {
				if idx.Reserve("d1") {
					if holders.Add(1) != 1 {
						t.Errorf("two goroutines hold the same driver")
					}
					holders.Add(-1) // must happen BEFORE Release
					if !idx.Release("d1") {
						t.Errorf("release failed for the holder")
					}
				}
			}
		}()
	}
	wg.Wait()
}

// The main test: 10,000 ride requests race for 100 drivers.
// (A pool of 500 goroutines handles the 10,000 requests, because Go's race
// detector crashes above 8,128 live goroutines.)
func TestReserveNearestNoDoubleBooking(t *testing.T) {
	const drivers, requests, workers = 100, 10000, 500

	idx := New(16, 6)
	for i := 0; i < drivers; i++ {
		// all within about 550 m of the pickup, so every request sees every driver
		idx.Upsert(fmt.Sprintf("d%d", i), "suv", 9.0+float64(i)*0.00005, 38.7)
	}

	jobs := make(chan struct{}, requests)
	for i := 0; i < requests; i++ {
		jobs <- struct{}{}
	}
	close(jobs)

	var (
		mu      sync.Mutex
		wins    = map[string]int{}
		winners atomic.Int32
		wg      sync.WaitGroup
		start   = make(chan struct{})
	)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for range jobs {
				m, ok := idx.ReserveNearest(9.0, 38.7, SearchOptions{PackageSlug: "suv", Limit: 5})
				if !ok {
					continue // all drivers are taken: this request loses
				}
				winners.Add(1)
				mu.Lock()
				wins[m.Driver.ID]++
				mu.Unlock()
			}
		}()
	}
	close(start)
	wg.Wait()

	if winners.Load() != drivers {
		t.Fatalf("%d requests won a driver, want exactly %d", winners.Load(), drivers)
	}
	if len(wins) != drivers {
		t.Fatalf("%d distinct drivers reserved, want %d", len(wins), drivers)
	}
	for id, n := range wins {
		if n != 1 {
			t.Fatalf("driver %s was reserved %d times (double booking)", id, n)
		}
	}
	for id := range wins {
		if d, _ := idx.Get(id); d.State != StateReserved {
			t.Fatalf("driver %s state = %v, want reserved", id, d.State)
		}
	}
	checkInvariant(t, idx)
	checkStates(t, idx)
}

func TestReservedDriverIsSkippedAndKeepsStateOnMove(t *testing.T) {
	idx := New(8, 6)
	idx.Upsert("d1", "suv", 9.000, 38.70)
	idx.Upsert("d2", "suv", 9.002, 38.70)

	idx.Reserve("d1")
	got := idx.Search(9.0, 38.7, SearchOptions{PackageSlug: "suv", Limit: 1})
	if len(got) != 1 || got[0].Driver.ID != "d2" {
		t.Fatalf("search returned %v, want [d2] (d1 is reserved)", ids(got))
	}

	// Move the reserved driver to a different cell: the state must follow.
	if ok, err := idx.UpdateLocation("d1", 9.10, 38.85); !ok || err != nil {
		t.Fatalf("update failed: %v %v", ok, err)
	}
	if d, _ := idx.Get("d1"); d.State != StateReserved {
		t.Fatalf("state lost on move: %v", d.State)
	}
	if got := idx.Search(9.10, 38.85, SearchOptions{PackageSlug: "suv", Limit: 1}); len(got) != 0 {
		t.Fatalf("reserved driver was returned: %v", ids(got))
	}

	idx.Release("d1")
	if got := idx.Search(9.10, 38.85, SearchOptions{PackageSlug: "suv", Limit: 1}); len(got) != 1 {
		t.Fatal("released driver should be searchable again")
	}
	checkStates(t, idx)
}

func TestRemoveBlocksReservation(t *testing.T) {
	idx := New(8, 6)
	idx.Upsert("d1", "suv", 9.0, 38.7)

	// Simulate a reservation that already looked the driver up before Remove.
	ds := idx.driverShardFor("d1")
	ds.mu.RLock()
	st := ds.states["d1"]
	ds.mu.RUnlock()

	idx.Remove("d1")

	if st.CompareAndSwap(int32(StateAvailable), int32(StateReserved)) {
		t.Fatal("a stale reservation won a driver who was removed")
	}
	if idx.Reserve("d1") {
		t.Fatal("removed driver must not be reservable")
	}
	checkStates(t, idx)
}

// Reservations racing with removals: no panic, and the maps stay consistent.
func TestReserveRaceWithRemove(t *testing.T) {
	idx := New(8, 6)
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		id := fmt.Sprintf("d%d", i)
		idx.Upsert(id, "suv", 9.0+float64(i)*0.0001, 38.7)
		wg.Add(2)
		go func() { defer wg.Done(); idx.Reserve(id) }()
		go func() { defer wg.Done(); idx.Remove(id) }()
	}
	wg.Wait()
	if idx.Len() != 0 {
		t.Fatalf("len = %d, want 0", idx.Len())
	}
	checkInvariant(t, idx)
	checkStates(t, idx)
}

// checkStates verifies the state map matches the driver map and that every
// stored driver shares its state pointer. Call it only when nothing else is
// running, because it takes locks in the opposite order of normal code.
func checkStates(t *testing.T, idx *Index) {
	t.Helper()
	for i := range idx.drivers {
		ds := &idx.drivers[i]
		ds.mu.RLock()
		if len(ds.states) != len(ds.cellOf) {
			t.Fatalf("shard %d: %d states but %d drivers", i, len(ds.states), len(ds.cellOf))
		}
		ds.mu.RUnlock()
	}
	for i := range idx.cells {
		cs := &idx.cells[i]
		cs.mu.RLock()
		for _, m := range cs.cells {
			for id, d := range m {
				ds := idx.driverShardFor(id)
				ds.mu.RLock()
				st := ds.states[id]
				ds.mu.RUnlock()
				if d.st != st {
					t.Fatalf("driver %s has a different state pointer than the state map", id)
				}
			}
		}
		cs.mu.RUnlock()
	}
}