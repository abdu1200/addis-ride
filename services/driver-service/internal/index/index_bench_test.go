package index

import (
	"fmt"
	"math/rand/v2"
	"sync/atomic"
	"testing"
)
// the hammering test of Feature 1
// shards=1 is the single-lock baseline; compare it against more shards.
//
//	go test -bench=. -benchmem -cpu=1,4,8 ./...
func runBench(b *testing.B, shards, readPercent int) {
	const n = 10000
	idx := New(shards, 6)

	ids := make([]string, n)
	r := rand.New(rand.NewPCG(42, 42))
	for i := range ids {
		ids[i] = fmt.Sprintf("driver-%d", i)
		lat, lng := randPoint(r)
		idx.Upsert(ids[i], "suv", lat, lng)
	}

	var seed atomic.Uint64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewPCG(seed.Add(1), 99))
		for pb.Next() {
			lat, lng := randPoint(r)
			if r.IntN(100) < readPercent {
				idx.CellDrivers(idx.Cell(lat, lng))
			} else {
				idx.Upsert(ids[r.IntN(n)], "suv", lat, lng)
			}
		}
	})
}

func BenchmarkUpsertOnly(b *testing.B) {
	for _, s := range []int{1, 16, 64, 256} {
		b.Run(fmt.Sprintf("shards=%d", s), func(b *testing.B) { runBench(b, s, 0) })
	}
}

// 80% writes (location updates), 20% reads (searches): the contention test.
func BenchmarkMixed80_20(b *testing.B) {
	for _, s := range []int{1, 16, 64, 256} {
		b.Run(fmt.Sprintf("shards=%d", s), func(b *testing.B) { runBench(b, s, 20) })
	}
}