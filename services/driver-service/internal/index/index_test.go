package index

import (
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
)
// unit + race tests
// A box around Addis Ababa: about 1000 geohash cells at precision 6,
// so drivers really do move between cells.
func randPoint(r *rand.Rand) (float64, float64) {
	return 8.9 + r.Float64()*0.2, 38.6 + r.Float64()*0.3
}

func TestUpsertAndGet(t *testing.T) {
	idx := New(8, 6)
	if err := idx.Upsert("d1", "suv", 9.03, 38.74); err != nil {
		t.Fatal(err)
	}
	d, ok := idx.Get("d1")
	if !ok || d.PackageSlug != "suv" || d.Lat != 9.03 || d.State != StateAvailable {
		t.Fatalf("unexpected driver: %+v ok=%v", d, ok)
	}
	if idx.Len() != 1 {
		t.Fatalf("len = %d, want 1", idx.Len())
	}
}

func TestInvalidInput(t *testing.T) {
	idx := New(8, 6)
	if idx.Upsert("", "suv", 9, 38) == nil {
		t.Fatal("empty id should fail")
	}
	if idx.Upsert("d1", "suv", 100, 38) == nil {
		t.Fatal("bad latitude should fail")
	}
	if idx.Upsert("d1", "suv", 9, 200) == nil {
		t.Fatal("bad longitude should fail")
	}
}

func TestMoveBetweenCells(t *testing.T) {
	idx := New(8, 6)
	idx.Upsert("d1", "suv", 9.00, 38.70)
	oldCell := idx.Cell(9.00, 38.70)

	idx.Upsert("d1", "suv", 9.10, 38.85)
	newCell := idx.Cell(9.10, 38.85)
	if oldCell == newCell {
		t.Fatal("test points must be in different cells")
	}
	if got := idx.CellDrivers(oldCell); len(got) != 0 {
		t.Fatalf("old cell should be empty, has %d", len(got))
	}
	if got := idx.CellDrivers(newCell); len(got) != 1 || got[0].ID != "d1" {
		t.Fatalf("new cell wrong: %+v", got)
	}
	checkInvariant(t, idx)
}

func TestStateSurvivesMove(t *testing.T) {
	idx := New(8, 6)
	idx.Upsert("d1", "suv", 9.00, 38.70)
	setState(idx, "d1", StateReserved)

	idx.Upsert("d1", "suv", 9.10, 38.85)
	got, _ := idx.Get("d1")
	if got.State != StateReserved {
		t.Fatalf("state lost on move: %v", got.State)
	}
}

func TestRemove(t *testing.T) {
	idx := New(8, 6)
	idx.Upsert("d1", "suv", 9.03, 38.74)
	if !idx.Remove("d1") {
		t.Fatal("remove should return true")
	}
	if idx.Remove("d1") {
		t.Fatal("second remove should return false")
	}
	if _, ok := idx.Get("d1"); ok {
		t.Fatal("driver should be gone")
	}
	checkInvariant(t, idx)
}

// Run with: go test -race ./...
func TestConcurrentUpsertRemove(t *testing.T) {
	const (
		workers = 32
		drivers = 200
		ops     = 5000
	)
	idx := New(16, 6)

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(seed uint64) {
			defer wg.Done()
			r := rand.New(rand.NewPCG(seed, 1))
			for i := 0; i < ops; i++ {
				id := fmt.Sprintf("d%d", r.IntN(drivers))
				lat, lng := randPoint(r)
				switch r.IntN(10) {
				case 0:
					idx.Remove(id)
				case 1, 2:
					idx.Get(id)
				case 3:
					idx.CellDrivers(idx.Cell(lat, lng))
				default:
					idx.Upsert(id, "suv", lat, lng)
				}
			}
		}(uint64(w + 1))
	}
	wg.Wait()

	checkInvariant(t, idx)
}

// Every driver must be in exactly one cell, and the two maps must agree.
func checkInvariant(t *testing.T, idx *Index) {
	t.Helper()
	seen := map[string]string{}

	for i := range idx.cells {
		cs := &idx.cells[i]
		cs.mu.RLock()
		for cell, m := range cs.cells {
			if len(m) == 0 {
				t.Fatalf("empty cell %s was not cleaned up", cell)
			}
			for id, d := range m {
				if prev, dup := seen[id]; dup {
					t.Fatalf("driver %s in two cells: %s and %s", id, prev, cell)
				}
				if d.CellID != cell {
					t.Fatalf("driver %s CellID %s != cell %s", id, d.CellID, cell)
				}
				seen[id] = cell
			}
		}
		cs.mu.RUnlock()
	}

	total := 0
	for i := range idx.drivers {
		ds := &idx.drivers[i]
		ds.mu.RLock()
		for id, cell := range ds.cellOf {
			total++
			if seen[id] != cell {
				t.Fatalf("driver %s: cellOf=%s but stored in %s", id, cell, seen[id])
			}
		}
		ds.mu.RUnlock()
	}
	if total != len(seen) {
		t.Fatalf("cellOf has %d drivers, cells have %d", total, len(seen))
	}
}

func TestUpdateLocationKeepsSlugAndState(t *testing.T) {
	idx := New(8, 6)
	idx.Upsert("d1", "suv", 9.00, 38.70)
	setState(idx, "d1", StateReserved)
 
	ok, err := idx.UpdateLocation("d1", 9.10, 38.85)
	if err != nil || !ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	d, _ := idx.Get("d1")
	if d.PackageSlug != "suv" || d.State != StateReserved || d.Lat != 9.10 ||
		d.CellID != idx.Cell(9.10, 38.85) {
		t.Fatalf("unexpected driver: %+v", d)
	}
	checkInvariant(t, idx)
}
 
func TestUpdateLocationUnknownDriver(t *testing.T) {
	idx := New(8, 6)
	ok, err := idx.UpdateLocation("ghost", 9.0, 38.7)
	if err != nil || ok {
		t.Fatalf("ok=%v err=%v, want false/nil", ok, err)
	}
	if idx.Len() != 0 {
		t.Fatal("UpdateLocation must never add a driver")
	}
}
 
func TestUpdateLocationInvalid(t *testing.T) {
	idx := New(8, 6)
	idx.Upsert("d1", "suv", 9.0, 38.7)
	if _, err := idx.UpdateLocation("d1", 100, 38.7); err == nil {
		t.Fatal("bad latitude should fail")
	}
}
 
// Run with: go test -race ./...
func TestUpdateLocationNeverResurrectsRemovedDriver(t *testing.T) {
	idx := New(8, 6)
	idx.Upsert("d1", "suv", 9.0, 38.7)
 
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		r := rand.New(rand.NewPCG(1, 1))
		for i := 0; i < 5000; i++ {
			lat, lng := randPoint(r)
			idx.UpdateLocation("d1", lat, lng)
		}
	}()
	idx.Remove("d1")
	wg.Wait()
 
	if idx.Len() != 0 {
		t.Fatalf("removed driver came back, len=%d", idx.Len())
	}
	checkInvariant(t, idx)
}
 