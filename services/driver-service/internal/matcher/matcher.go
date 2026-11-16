// Package matcher runs ride-request matching on a fixed pool of workers.
//
//	request --Submit--> [ bounded queue ] --> worker --> search + reserve --> result
//
// Why a pool instead of one goroutine per request:
//   - the number of searches running at once is fixed (Workers), so a traffic
//     spike can't spawn thousands of CPU-hungry goroutines;
//   - the queue is bounded, so when we are overloaded new requests are rejected
//     immediately (ErrQueueFull) instead of piling up in memory (backpressure).
package matcher

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"ride-sharing/services/driver-service/internal/index"
)

var (
	// ErrQueueFull means we are overloaded: reject now, don't wait.
	ErrQueueFull = errors.New("matcher: queue full")
	// ErrClosed means the pool is shutting down.
	ErrClosed = errors.New("matcher: closed")
)

type Config struct {
	Workers         int     // number of worker goroutines (0 = GOMAXPROCS)
	QueueSize       int     // max waiting requests (0 = 1024)
	SearchLimit     int     // drivers to consider per search (0 = 5)
	MaxRadiusMeters float64 // search radius (0 = 5000)
	BeforeSearch    func() // optional: runs in the worker before every search (used by the simulator)
}

// Request is one ride request to match.
type Request struct {
	TripID      string
	PackageSlug string
	Lat, Lng    float64 // pickup point
	Exclude     map[string]struct{} // drivers to skip
}

// Result is the outcome of one request.
//
//	Err != nil          -> cancelled or timed out; no driver is held
//	Matched == true     -> DriverID is RESERVED for this request
//	Matched == false    -> no available driver nearby (not an error)
type Result struct {
	Matched        bool
	DriverID       string
	DistanceMeters float64
	Err            error

	QueueWait   time.Duration // time spent waiting in the queue
	ProcessTime time.Duration // time spent searching + reserving
}

type job struct {
	ctx      context.Context
	req      Request
	enqueued time.Time
	resp     chan Result // buffered (1): a worker never blocks on a caller who left
}

type Stats struct {
	Submitted int64
	Rejected  int64
	Matched   int64
	NoDriver  int64
	Cancelled int64
	QueueLen  int
}

// Matcher was a 'Pool' in an earlier version of this code.
type Matcher struct {
	idx   *index.Index
	cfg   Config
	queue chan job

	mu     sync.RWMutex // guards closed, so Submit can never send on a closed channel
	closed bool
	wg     sync.WaitGroup

	submitted, rejected, matched, noDriver, cancelled atomic.Int64
}


// New starts the workers.
func New(idx *index.Index, cfg Config) *Matcher {
	if cfg.Workers <= 0 {
		cfg.Workers = runtime.GOMAXPROCS(0)
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 1024
	}
	if cfg.SearchLimit <= 0 {
		cfg.SearchLimit = 5
	}
	if cfg.MaxRadiusMeters <= 0 {
		cfg.MaxRadiusMeters = 5000
	}

	p := &Matcher{idx: idx, cfg: cfg, queue: make(chan job, cfg.QueueSize)}
	p.wg.Add(cfg.Workers)
	for i := 0; i < cfg.Workers; i++ {
		go p.worker()
	}
	return p
}

// Submit puts a request in the queue WITHOUT waiting. It returns ErrQueueFull
// right away if the queue is full. Read the returned channel exactly once; if
// the result has Matched=true, that driver is reserved and the caller owns it
// (it must Assign it or Release it).
func (p *Matcher) Submit(ctx context.Context, req Request) (<-chan Result, error) {
	j := job{ctx: ctx, req: req, enqueued: time.Now(), resp: make(chan Result, 1)}

	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return nil, ErrClosed
	}
	select {
	case p.queue <- j:
		p.submitted.Add(1)
		return j.resp, nil
	default:
		p.rejected.Add(1)
		return nil, ErrQueueFull
	}
}

// Match submits a request and waits for the result or for ctx to end.
// If ctx ends first, any driver the matcher reserves for us is released.
func (p *Matcher) Match(ctx context.Context, req Request) (Result, error) {
	ch, err := p.Submit(ctx, req)
	if err != nil {
		return Result{}, err
	}
	select {
	case res := <-ch:
		return res, res.Err
	case <-ctx.Done():
		// A worker may still reserve a driver for us a moment from now.
		// Give it back when it does, so nobody stays reserved forever.
		go func() {
			if res := <-ch; res.Matched {
				p.idx.Release(res.DriverID)
			}
		}()
		return Result{}, ctx.Err()
	}
}

// Close stops accepting requests, lets the workers finish everything already
// queued, and waits for them to exit.
func (p *Matcher) Close() {
	p.mu.Lock()
	if !p.closed {
		p.closed = true
		close(p.queue)
	}
	p.mu.Unlock()
	p.wg.Wait()
}

func (p *Matcher) Stats() Stats {
	return Stats{
		Submitted: p.submitted.Load(),
		Rejected:  p.rejected.Load(),
		Matched:   p.matched.Load(),
		NoDriver:  p.noDriver.Load(),
		Cancelled: p.cancelled.Load(),
		QueueLen:  len(p.queue),
	}
}

func (p *Matcher) worker() {
	defer p.wg.Done()

	for j := range p.queue {
		wait := time.Since(j.enqueued)

		// The rider gave up while waiting: don't waste a search on them.
		if err := j.ctx.Err(); err != nil {
			p.cancelled.Add(1)
			j.resp <- Result{Err: err, QueueWait: wait}
			continue
		}

		start := time.Now()

		if p.cfg.BeforeSearch != nil {
			p.cfg.BeforeSearch()
		}

		m, ok := p.idx.ReserveNearest(j.req.Lat, j.req.Lng, index.SearchOptions{
			PackageSlug:     j.req.PackageSlug,
			Limit:           p.cfg.SearchLimit,
			MaxRadiusMeters: p.cfg.MaxRadiusMeters,
			Exclude:         j.req.Exclude,
		})
		res := Result{QueueWait: wait, ProcessTime: time.Since(start)}

		if !ok {
			p.noDriver.Add(1)
			j.resp <- res
			continue
		}

		// The rider gave up during the search: give the driver back.
		if err := j.ctx.Err(); err != nil {
			p.idx.Release(m.Driver.ID)
			p.cancelled.Add(1)
			res.Err = err
			j.resp <- res
			continue
		}

		res.Matched = true
		res.DriverID = m.Driver.ID
		res.DistanceMeters = m.DistanceMeters
		p.matched.Add(1)
		j.resp <- res
	}
}