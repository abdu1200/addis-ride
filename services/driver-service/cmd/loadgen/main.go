// Command loadgen simulates N drivers, each sending one location update every
// `interval`, against the in-process geo-index, and reports throughput,
// p50/p95/p99 Upsert latency, memory and GC stats.
//
// eg: go run ./cmd/loadgen -drivers=10000 -interval=4s -duration=60s -shards=256

// the realistic updates test of Feature 2
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"ride-sharing/services/driver-service/internal/index"
)

var packages = []string{"sedan", "suv", "van", "luxury"}

type result struct {
	Lang           string  `json:"lang"`
	Drivers        int     `json:"drivers"`
	Shards         int     `json:"shards"`
	IntervalSec    float64 `json:"interval_sec"`
	DurationSec    float64 `json:"duration_sec"`
	Updates        int64   `json:"updates"`
	CellMoves      int64   `json:"cell_moves"`
	Errors         int64   `json:"errors"`
	ExpectedPerSec float64 `json:"expected_updates_per_sec"`
	ActualPerSec   float64 `json:"actual_updates_per_sec"`
	P50us          float64 `json:"p50_us"`
	P95us          float64 `json:"p95_us"`
	P99us          float64 `json:"p99_us"`
	MaxUs          float64 `json:"max_us"`
	PeakHeapMB     float64 `json:"peak_heap_mb"`
	NumGC          uint32  `json:"num_gc"`
	MaxGCPauseUs   float64 `json:"max_gc_pause_us"`
	IndexSize      int     `json:"index_size"`
	GOMAXPROCS     int     `json:"gomaxprocs"`
}

func main() {
	drivers := flag.Int("drivers", 10000, "number of simulated drivers")
	interval := flag.Duration("interval", 4*time.Second, "time between updates per driver")
	duration := flag.Duration("duration", 60*time.Second, "total test duration")
	shards := flag.Int("shards", index.DefaultShards, "index shard count (1 = single-lock baseline)")
	asJSON := flag.Bool("json", false, "print one JSON line instead of the table")
	flag.Parse()

	if *interval <= 0 || *drivers <= 0 {
		fmt.Fprintln(os.Stderr, "drivers and interval must be positive")
		os.Exit(1)
	}

	idx := index.New(*shards, index.DefaultPrecision)

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		all       = make([]time.Duration, 0, *drivers*(int(*duration / *interval)+1))
		updates   atomic.Int64
		cellMoves atomic.Int64
		errCount  atomic.Int64
		peakHeap  atomic.Uint64
	)

	// Sample heap usage once per second to find the peak.
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		var ms runtime.MemStats
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				runtime.ReadMemStats(&ms)
				if ms.HeapAlloc > peakHeap.Load() {
					peakHeap.Store(ms.HeapAlloc)
				}
			}
		}
	}()

	started := time.Now()

	for i := 0; i < *drivers; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()

			r := rand.New(rand.NewPCG(uint64(n)+1, 7))
			id := fmt.Sprintf("driver-%d", n)
			pkg := packages[n%len(packages)]

			// Start somewhere in the Addis Ababa box.
			lat := 8.9 + r.Float64()*0.2
			lng := 38.6 + r.Float64()*0.3
			cell := idx.Cell(lat, lng)

			samples := make([]time.Duration, 0, int(*duration / *interval)+1)
			defer func() {
				mu.Lock()
				all = append(all, samples...)
				mu.Unlock()
			}()

			// Random start offset so drivers don't all fire in the same instant.
			if !sleep(ctx, time.Duration(r.Int64N(int64(*interval)))) {
				return
			}

			ticker := time.NewTicker(*interval)
			defer ticker.Stop()

			for {
				// Small random walk: up to ~45 m per step.
				lat += (r.Float64() - 0.5) * 0.0008
				lng += (r.Float64() - 0.5) * 0.0008

				start := time.Now()
				err := idx.Upsert(id, pkg, lat, lng)
				samples = append(samples, time.Since(start))

				updates.Add(1)
				if err != nil {
					errCount.Add(1)
				}
				if c := idx.Cell(lat, lng); c != cell {
					cell = c
					cellMoves.Add(1)
				}

				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
				}
			}
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(started)

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	if ms.HeapAlloc > peakHeap.Load() {
		peakHeap.Store(ms.HeapAlloc)
	}
	var maxPause uint64
	n := ms.NumGC
	if n > 256 {
		n = 256
	}
	for i := uint32(0); i < n; i++ {
		if p := ms.PauseNs[i]; p > maxPause {
			maxPause = p
		}
	}

	slices.Sort(all)

	res := result{
		Lang:           "go",
		Drivers:        *drivers,
		Shards:         *shards,
		IntervalSec:    interval.Seconds(),
		DurationSec:    elapsed.Seconds(),
		Updates:        updates.Load(),
		CellMoves:      cellMoves.Load(),
		Errors:         errCount.Load(),
		ExpectedPerSec: float64(*drivers) / interval.Seconds(),
		ActualPerSec:   float64(updates.Load()) / elapsed.Seconds(),
		P50us:          us(percentile(all, 50)),
		P95us:          us(percentile(all, 95)),
		P99us:          us(percentile(all, 99)),
		MaxUs:          us(percentile(all, 100)),
		PeakHeapMB:     float64(peakHeap.Load()) / (1 << 20),
		NumGC:          ms.NumGC,
		MaxGCPauseUs:   float64(maxPause) / 1000,
		IndexSize:      idx.Len(),
		GOMAXPROCS:     runtime.GOMAXPROCS(0),
	}

	if *asJSON {
		_ = json.NewEncoder(os.Stdout).Encode(res)
		return
	}
	printTable(res)
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// percentile expects a sorted slice.
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := int(math.Ceil(p/100*float64(len(sorted)))) - 1
	if i < 0 {
		i = 0
	}
	return sorted[i]
}

func us(d time.Duration) float64 { return float64(d.Nanoseconds()) / 1000 }

func printTable(r result) {
	fmt.Printf("\n=== Go ingestion load test ===\n")
	fmt.Printf("drivers:            %d (every %.0fs)\n", r.Drivers, r.IntervalSec)
	fmt.Printf("shards:             %d   GOMAXPROCS: %d\n", r.Shards, r.GOMAXPROCS)
	fmt.Printf("duration:           %.1fs\n", r.DurationSec)
	fmt.Printf("updates:            %d (%d moved cells, %d errors)\n", r.Updates, r.CellMoves, r.Errors)
	fmt.Printf("throughput:         %.0f updates/sec (target %.0f)\n", r.ActualPerSec, r.ExpectedPerSec)
	fmt.Printf("latency p50/p95/p99: %.1f / %.1f / %.1f us   max %.1f us\n", r.P50us, r.P95us, r.P99us, r.MaxUs)
	fmt.Printf("peak heap:          %.1f MB\n", r.PeakHeapMB)
	fmt.Printf("GC:                 %d runs, max pause %.0f us\n", r.NumGC, r.MaxGCPauseUs)
	fmt.Printf("index size at end:  %d (should equal drivers)\n\n", r.IndexSize)
}