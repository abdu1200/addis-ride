package index

import (
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"sort"
	"sync"
	"testing"

	"github.com/mmcloughlin/geohash"
)

func setState(idx *Index, id string, s State) {
	ds := idx.driverShardFor(id)
	ds.mu.RLock()
	st := ds.states[id]
	ds.mu.RUnlock()
	st.Store(int32(s))
}

func ids(ms []Match) []string {
	out := make([]string, len(ms))
	for i, m := range ms {
		out[i] = m.Driver.ID
	}
	return out
}

func TestHaversine(t *testing.T) {
	// 0.001 degrees of latitude is about 111 m.
	d := Haversine(9.0, 38.7, 9.001, 38.7)
	if math.Abs(d-111.2) > 1 {
		t.Fatalf("got %.1f m, want about 111.2", d)
	}
	if Haversine(9, 38, 9, 38) != 0 {
		t.Fatal("same point should be 0")
	}
}

func TestSearchOrderLimitAndFilters(t *testing.T) {
	idx := New(8, 6)
	idx.Upsert("c", "suv", 9.010, 38.7)   // about 1.1 km
	idx.Upsert("a", "suv", 9.001, 38.7)   // about 111 m
	idx.Upsert("b", "suv", 9.002, 38.7)   // about 222 m
	idx.Upsert("van1", "van", 9.0005, 38.7) // closest, but a different slug

	got := idx.Search(9.0, 38.7, SearchOptions{PackageSlug: "suv", Limit: 2})
	if !slices.Equal(ids(got), []string{"a", "b"}) {
		t.Fatalf("got %v, want [a b]", ids(got))
	}
	if math.Abs(got[0].DistanceMeters-111.2) > 2 {
		t.Fatalf("distance a = %.1f", got[0].DistanceMeters)
	}

	// Reserved drivers must be skipped.
	setState(idx, "a", StateReserved)
	got = idx.Search(9.0, 38.7, SearchOptions{PackageSlug: "suv", Limit: 5})
	if !slices.Equal(ids(got), []string{"b", "c"}) {
		t.Fatalf("got %v, want [b c]", ids(got))
	}

	// Empty slug means any package.
	got = idx.Search(9.0, 38.7, SearchOptions{Limit: 1})
	if !slices.Equal(ids(got), []string{"van1"}) {
		t.Fatalf("got %v, want [van1]", ids(got))
	}

	// Radius cuts off far drivers.
	got = idx.Search(9.0, 38.7, SearchOptions{PackageSlug: "suv", Limit: 5, MaxRadiusMeters: 500})
	if !slices.Equal(ids(got), []string{"b"}) {
		t.Fatalf("got %v, want [b]", ids(got))
	}

	// Nothing found, bad input.
	if got := idx.Search(20, 20, SearchOptions{Limit: 3}); len(got) != 0 {
		t.Fatalf("expected no results, got %v", ids(got))
	}
	if idx.Search(9, 38, SearchOptions{Limit: 0}) != nil {
		t.Fatal("limit 0 should return nil")
	}
}

// The closest driver is in a NEIGHBOR cell while another driver is far away in
// the pickup's own cell. Scanning only the pickup cell would get this wrong.
func TestSearchFindsCloserDriverInNeighborCell(t *testing.T) {
	idx := New(8, 6)
	cell := idx.Cell(9.0, 38.7)
	box := geohash.BoundingBox(cell)
	cLat, _ := box.Center()

	pickupLng := box.MinLng + 0.00005
	idx.Upsert("near-west", "suv", cLat, box.MinLng-0.00005) // about 11 m, other cell
	idx.Upsert("far-east", "suv", cLat, box.MaxLng-0.0001)   // about 1.2 km, same cell

	if idx.Cell(cLat, box.MinLng-0.00005) == cell {
		t.Fatal("test setup: near-west should be in a different cell")
	}

	got := idx.Search(cLat, pickupLng, SearchOptions{PackageSlug: "suv", Limit: 1})
	if len(got) != 1 || got[0].Driver.ID != "near-west" {
		t.Fatalf("got %v, want [near-west]", ids(got))
	}
}

// Compare Search against a slow brute-force scan of every driver.
func TestSearchMatchesBruteForce(t *testing.T) {
	const (
		n      = 2000
		limit  = 5
		radius = 3000.0
	)
	idx := New(16, 6)
	r := rand.New(rand.NewPCG(1, 2))

	type pt struct {
		id       string
		lat, lng float64
	}
	pts := make([]pt, n)
	for i := range pts {
		lat, lng := randPoint(r)
		pts[i] = pt{fmt.Sprintf("d%d", i), lat, lng}
		idx.Upsert(pts[i].id, "suv", lat, lng)
	}

	type dd struct {
		id string
		d  float64
	}
	for q := 0; q < 100; q++ {
		qLat, qLng := randPoint(r)

		got := idx.Search(qLat, qLng, SearchOptions{PackageSlug: "suv", Limit: limit, MaxRadiusMeters: radius})

		var want []dd
		for _, p := range pts {
			if d := Haversine(qLat, qLng, p.lat, p.lng); d <= radius {
				want = append(want, dd{p.id, d})
			}
		}
		sort.Slice(want, func(i, j int) bool { return want[i].d < want[j].d })
		if len(want) > limit {
			want = want[:limit]
		}

		if len(got) != len(want) {
			t.Fatalf("query %d: got %d results, want %d", q, len(got), len(want))
		}
		for i := range want {
			if got[i].Driver.ID != want[i].id {
				t.Fatalf("query %d, rank %d: got %s, want %s", q, i, got[i].Driver.ID, want[i].id)
			}
		}
	}
}

// Run with: go test -race ./...
func TestSearchConcurrentWithUpdates(t *testing.T) {
	idx := New(16, 6)
	var wg sync.WaitGroup

	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(seed uint64) {
			defer wg.Done()
			r := rand.New(rand.NewPCG(seed, 1))
			for i := 0; i < 3000; i++ {
				id := fmt.Sprintf("d%d", r.IntN(500))
				lat, lng := randPoint(r)
				if r.IntN(10) == 0 {
					idx.Remove(id)
				} else {
					idx.Upsert(id, "suv", lat, lng)
				}
			}
		}(uint64(w + 1))
	}

	for s := 0; s < 8; s++ {
		wg.Add(1)
		go func(seed uint64) {
			defer wg.Done()
			r := rand.New(rand.NewPCG(seed, 2))
			for i := 0; i < 500; i++ {
				lat, lng := randPoint(r)
				res := idx.Search(lat, lng, SearchOptions{PackageSlug: "suv", Limit: 10, MaxRadiusMeters: 3000})

				seen := map[string]bool{}
				for j, m := range res {
					if seen[m.Driver.ID] {
						t.Errorf("duplicate driver %s in one result", m.Driver.ID)
						return
					}
					seen[m.Driver.ID] = true
					if j > 0 && m.DistanceMeters < res[j-1].DistanceMeters {
						t.Errorf("results not sorted")
						return
					}
				}
			}
		}(uint64(s + 100))
	}

	wg.Wait()
	checkInvariant(t, idx)
}