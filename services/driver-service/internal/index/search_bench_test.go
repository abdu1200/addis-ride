package index

import (
	"fmt"
	"math/rand/v2"
	"sync/atomic"
	"testing"
)

// search test of feature 3 
// search test with hammering riders search request

var slugs = []string{"sedan", "suv", "van", "luxury"}

func buildIndex(shards, n int) (*Index, []string) {
	idx := New(shards, 6)
	r := rand.New(rand.NewPCG(42, 42))
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("driver-%d", i)
		lat, lng := randPoint(r)
		idx.Upsert(ids[i], slugs[i%len(slugs)], lat, lng)
	}
	return idx, ids
}

var searchOpts = SearchOptions{PackageSlug: "suv", Limit: 5, MaxRadiusMeters: 3000}

// Raw CPU cost of ONE search on one goroutine. This is the cleanest number
// to compare Go against Rust.
func BenchmarkSearchSingleThread(b *testing.B) {
	for _, n := range []int{10000, 50000, 100000} {
		b.Run(fmt.Sprintf("drivers=%d", n), func(b *testing.B) {
			idx, _ := buildIndex(256, n)
			r := rand.New(rand.NewPCG(7, 7))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				lat, lng := randPoint(r)
				idx.Search(lat, lng, searchOpts)
			}
		})
	}
}

// Many goroutines searching at once (read-heavy contention).
func BenchmarkSearchParallel(b *testing.B) {
	for _, n := range []int{10000, 50000, 100000} {
		for _, s := range []int{1, 256} {
			b.Run(fmt.Sprintf("drivers=%d/shards=%d", n, s), func(b *testing.B) {
				idx, _ := buildIndex(s, n)
				var seed atomic.Uint64
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					r := rand.New(rand.NewPCG(seed.Add(1), 5))
					for pb.Next() {
						lat, lng := randPoint(r)
						idx.Search(lat, lng, searchOpts)
					}
				})
			})
		}
	}
}

// 80% location updates + 20% searches at the same time on 100k drivers:
// the real contention test for the whole index.
func BenchmarkMixedUpsertSearch(b *testing.B) {
	const n = 100000
	for _, s := range []int{1, 256} {
		b.Run(fmt.Sprintf("drivers=%d/shards=%d", n, s), func(b *testing.B) {
			idx, ids := buildIndex(s, n)
			var seed atomic.Uint64
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				r := rand.New(rand.NewPCG(seed.Add(1), 9))
				for pb.Next() {
					lat, lng := randPoint(r)
					if r.IntN(100) < 20 {
						idx.Search(lat, lng, searchOpts)
					} else {
						i := r.IntN(n)
						idx.Upsert(ids[i], slugs[i%len(slugs)], lat, lng)
					}
				}
			})
		})
	}
}