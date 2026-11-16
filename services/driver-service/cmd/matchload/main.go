// Command matchload sends ride requests to the worker matcher at a fixed rate
// (requests per second) and reports throughput, rejections, end-to-end latency
// (p50/p95/p99), queue wait, memory and GC.
//
// It is an "open-loop" test: requests arrive on schedule whether or not the
// matcher keeps up, like real riders. When the matcher is overloaded you will see
// the queue fill, latency rise, and then requests being rejected.
//
// eg: go run ./cmd/matchload -workers=8 -rate=4000 -duration=20s

// the realistic matcher test of Feature 5 
package main

import (
	"context"
	"encoding/json"
	"errors"
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
	"ride-sharing/services/driver-service/internal/matcher"
)

var slugs = []string{"sedan", "suv", "van", "luxury"}

type sample struct{ total, wait, proc time.Duration }

type result struct {
	Lang            string  `json:"lang"`
	Drivers         int     `json:"drivers"`
	Shards          int     `json:"shards"`
	Workers         int     `json:"workers"`
	QueueSize       int     `json:"queue_size"`
	TargetRate      int     `json:"target_rate"`
	DurationSec     float64 `json:"duration_sec"`
	Sent            int64   `json:"sent"`
	Accepted        int64   `json:"accepted"`
	Rejected        int64   `json:"rejected"`
	RejectPct       float64 `json:"reject_pct"`
	Matched         int64   `json:"matched"`
	NoDriver        int64   `json:"no_driver"`
	Failed          int64   `json:"failed"`
	CompletedPerSec float64 `json:"completed_per_sec"`
	P50us           float64 `json:"p50_us"`
	P95us           float64 `json:"p95_us"`
	P99us           float64 `json:"p99_us"`
	MaxUs           float64 `json:"max_us"`
	QueueWaitP99us  float64 `json:"queue_wait_p99_us"`
	ProcessP50us    float64 `json:"process_p50_us"`
	PeakHeapMB      float64 `json:"peak_heap_mb"`
	NumGC           uint32  `json:"num_gc"`
	MaxGCPauseUs    float64 `json:"max_gc_pause_us"`
	GOMAXPROCS      int     `json:"gomaxprocs"`
}

func main() {
	drivers := flag.Int("drivers", 100000, "drivers loaded in the index")
	shards := flag.Int("shards", 256, "index shard count")
	workers := flag.Int("workers", 8, "worker goroutines in the pool")
	queue := flag.Int("queue", 1024, "bounded queue size")
	rate := flag.Int("rate", 4000, "ride requests per second")
	duration := flag.Duration("duration", 20*time.Second, "how long to send requests")
	hold := flag.Duration("hold", 0, "keep a matched driver busy this long before releasing (0 = release at once)")
	asJSON := flag.Bool("json", false, "print one JSON line instead of the table")
	flag.Parse()

	if *rate <= 0 || *drivers <= 0 {
		fmt.Fprintln(os.Stderr, "rate and drivers must be positive")
		os.Exit(1)
	}

	// 1. Load the index.
	idx := index.New(*shards, index.DefaultPrecision)
	r := rand.New(rand.NewPCG(42, 42))
	for i := 0; i < *drivers; i++ {
		lat, lng := point(r)
		idx.Upsert(fmt.Sprintf("driver-%d", i), slugs[i%len(slugs)], lat, lng)
	}

	// 2. Start the matcher.
	mat := matcher.New(idx, matcher.Config{
		Workers:         *workers,
		QueueSize:       *queue,
		SearchLimit:     5,
		MaxRadiusMeters: 3000,
	})

	var (
		accepted, rejected, matched, noDriver, failed atomic.Int64
		peakHeap                                      atomic.Uint64
		reqWG                                         sync.WaitGroup
		samples                                       []sample
		sampleCh                                      = make(chan sample, 4096)
		collectorDone                                 = make(chan struct{})
		stopSampler                                   = make(chan struct{})
	)

	// Collect latency samples in one goroutine.
	go func() {
		for s := range sampleCh {
			samples = append(samples, s)
		}
		close(collectorDone)
	}()

	// Track peak heap once per second.
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		var ms runtime.MemStats
		for {
			select {
			case <-stopSampler:
				return
			case <-t.C:
				runtime.ReadMemStats(&ms)
				if ms.HeapAlloc > peakHeap.Load() {
					peakHeap.Store(ms.HeapAlloc)
				}
			}
		}
	}()

	release := func(id string) {
		if *hold > 0 {
			time.AfterFunc(*hold, func() { idx.Release(id) })
			return
		}
		idx.Release(id)
	}

	// 3. Send requests on a fixed schedule.
	genCtx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	start := time.Now()
	tick := time.NewTicker(time.Millisecond)
	sent := 0

generate:
	for {
		select {
		case <-genCtx.Done():
			break generate
		case now := <-tick.C:
			// How many requests should exist by now? Send the missing ones.
			target := int(now.Sub(start).Seconds() * float64(*rate))
			for ; sent < target; sent++ {
				lat, lng := point(r)
				req := matcher.Request{
					PackageSlug: slugs[r.IntN(len(slugs))],
					Lat:         lat,
					Lng:         lng,
				}

				t0 := time.Now()
				ch, err := mat.Submit(context.Background(), req)
				if err != nil {
					if errors.Is(err, matcher.ErrQueueFull) {
						rejected.Add(1)
					} else {
						failed.Add(1)
					}
					continue
				}

				accepted.Add(1)
				reqWG.Add(1)
				go func() {
					defer reqWG.Done()
					res := <-ch
					sampleCh <- sample{time.Since(t0), res.QueueWait, res.ProcessTime}
					switch {
					case res.Err != nil:
						failed.Add(1)
					case res.Matched:
						matched.Add(1)
						release(res.DriverID)
					default:
						noDriver.Add(1)
					}
				}()
			}
		}
	}
	tick.Stop()

	// 4. Let everything in the queue finish, then stop.
	reqWG.Wait()
	elapsed := time.Since(start)
	mat.Close()
	close(sampleCh)
	<-collectorDone
	close(stopSampler)

	// 5. Report.
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

	totals := make([]time.Duration, len(samples))
	waits := make([]time.Duration, len(samples))
	procs := make([]time.Duration, len(samples))
	for i, s := range samples {
		totals[i], waits[i], procs[i] = s.total, s.wait, s.proc
	}
	slices.Sort(totals)
	slices.Sort(waits)
	slices.Sort(procs)

	sentCount := int64(sent)
	completed := matched.Load() + noDriver.Load()

	res := result{
		Lang:            "go",
		Drivers:         *drivers,
		Shards:          *shards,
		Workers:         *workers,
		QueueSize:       *queue,
		TargetRate:      *rate,
		DurationSec:     elapsed.Seconds(),
		Sent:            sentCount,
		Accepted:        accepted.Load(),
		Rejected:        rejected.Load(),
		RejectPct:       100 * float64(rejected.Load()) / math.Max(1, float64(sentCount)),
		Matched:         matched.Load(),
		NoDriver:        noDriver.Load(),
		Failed:          failed.Load(),
		CompletedPerSec: float64(completed) / elapsed.Seconds(),
		P50us:           us(percentile(totals, 50)),
		P95us:           us(percentile(totals, 95)),
		P99us:           us(percentile(totals, 99)),
		MaxUs:           us(percentile(totals, 100)),
		QueueWaitP99us:  us(percentile(waits, 99)),
		ProcessP50us:    us(percentile(procs, 50)),
		PeakHeapMB:      float64(peakHeap.Load()) / (1 << 20),
		NumGC:           ms.NumGC,
		MaxGCPauseUs:    float64(maxPause) / 1000,
		GOMAXPROCS:      runtime.GOMAXPROCS(0),
	}

	if *asJSON {
		_ = json.NewEncoder(os.Stdout).Encode(res)
		return
	}
	printTable(res)
}

func point(r *rand.Rand) (float64, float64) {
	return 8.9 + r.Float64()*0.2, 38.6 + r.Float64()*0.3
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
	fmt.Printf("\n=== Go worker-pool load test ===\n")
	fmt.Printf("drivers: %d   shards: %d   workers: %d   queue: %d   GOMAXPROCS: %d\n",
		r.Drivers, r.Shards, r.Workers, r.QueueSize, r.GOMAXPROCS)
	fmt.Printf("target rate:        %d requests/sec for %.1fs\n", r.TargetRate, r.DurationSec)
	fmt.Printf("sent:               %d (accepted %d, rejected %d = %.1f%%)\n", r.Sent, r.Accepted, r.Rejected, r.RejectPct)
	fmt.Printf("results:            %d matched, %d no driver nearby, %d failed\n", r.Matched, r.NoDriver, r.Failed)
	fmt.Printf("throughput:         %.0f completed requests/sec\n", r.CompletedPerSec)
	fmt.Printf("latency p50/p95/p99: %.0f / %.0f / %.0f us   max %.0f us   (submit -> result)\n", r.P50us, r.P95us, r.P99us, r.MaxUs)
	fmt.Printf("queue wait p99:     %.0f us   process p50: %.0f us\n", r.QueueWaitP99us, r.ProcessP50us)
	fmt.Printf("peak heap:          %.1f MB\n", r.PeakHeapMB)
	fmt.Printf("GC:                 %d runs, max pause %.0f us\n\n", r.NumGC, r.MaxGCPauseUs)
}