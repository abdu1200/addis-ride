package matcher

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync/atomic"
	"testing"

	"ride-sharing/services/driver-service/internal/index"
)

var benchSlugs = []string{"sedan", "suv", "van", "luxury"}

func benchPoint(r *rand.Rand) (float64, float64) {
	return 8.9 + r.Float64()*0.2, 38.6 + r.Float64()*0.3
}

func buildBenchIndex(n int) *index.Index {
	idx := index.New(256, 6)
	r := rand.New(rand.NewPCG(42, 42))
	for i := 0; i < n; i++ {
		lat, lng := benchPoint(r)
		idx.Upsert(fmt.Sprintf("driver-%d", i), benchSlugs[i%len(benchSlugs)], lat, lng)
	}
	return idx
}

// Baseline: callers run search + reserve themselves, no matcher.
func BenchmarkDirect(b *testing.B) {
	idx := buildBenchIndex(100000)
	opts := index.SearchOptions{PackageSlug: "suv", Limit: 5, MaxRadiusMeters: 3000}
	var seed atomic.Uint64

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewPCG(seed.Add(1), 5))
		for pb.Next() {
			lat, lng := benchPoint(r)
			if m, ok := idx.ReserveNearest(lat, lng, opts); ok {
				idx.Release(m.Driver.ID)
			}
		}
	})
}

// Same work, but every request goes through the queue and a worker.
// The gap to BenchmarkDirect is the cost of the channel hand-offs.
func BenchmarkMatcher(b *testing.B) {
	idx := buildBenchIndex(100000)

	for _, workers := range []int{1, 8, 32} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			p := New(idx, Config{Workers: workers, QueueSize: 4096, SearchLimit: 5, MaxRadiusMeters: 3000})
			defer p.Close()

			var seed atomic.Uint64
			ctx := context.Background()

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				r := rand.New(rand.NewPCG(seed.Add(1), 6))
				for pb.Next() {
					lat, lng := benchPoint(r)
					res, err := p.Match(ctx, Request{PackageSlug: "suv", Lat: lat, Lng: lng})
					if err == nil && res.Matched {
						idx.Release(res.DriverID)
					}
				}
			})
		})
	}
}