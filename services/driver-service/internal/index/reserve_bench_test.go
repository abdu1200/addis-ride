package index

import (
	"fmt"
	"math/rand/v2"
	"sync/atomic"
	"testing"
)

// Hammering test of Feature 4: reservation test (reserve a driver, then release it) with low contention (100,000 drivers) and high contention (100 drivers).
// Hammering test of Feature 4: full match test (search + reserve + release) cycle.

// Uses buildIndex and searchOpts from search_bench_test.go.

// Cost of one atomic reserve + release on random drivers (low contention).
func BenchmarkReserveRelease(b *testing.B) {
	idx, ids := buildIndex(256, 100000)
	var seed atomic.Uint64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewPCG(seed.Add(1), 3))
		for pb.Next() {
			id := ids[r.IntN(len(ids))]
			if idx.Reserve(id) {
				idx.Release(id)
			}
		}
	})
}

// Same, but only 100 drivers, so goroutines constantly fight over the same ones.
func BenchmarkReserveContention(b *testing.B) {
	idx, ids := buildIndex(256, 100)
	var seed atomic.Uint64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewPCG(seed.Add(1), 4))
		for pb.Next() {
			id := ids[r.IntN(len(ids))]
			if idx.Reserve(id) {
				idx.Release(id)
			}
		}
	})
}

// One whole match: search + reserve the nearest free driver, then release it.
// This is the operation Feature 5's worker pool will run for every ride request.
func BenchmarkMatchOne(b *testing.B) {
	for _, s := range []int{1, 256} {
		b.Run(fmt.Sprintf("drivers=100000/shards=%d", s), func(b *testing.B) {
			idx, _ := buildIndex(s, 100000)
			var seed atomic.Uint64
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				r := rand.New(rand.NewPCG(seed.Add(1), 6))
				for pb.Next() {
					lat, lng := randPoint(r)
					if m, ok := idx.ReserveNearest(lat, lng, searchOpts); ok {
						idx.Release(m.Driver.ID)
					}
				}
			})
		})
	}
}