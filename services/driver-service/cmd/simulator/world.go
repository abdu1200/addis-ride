package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"ride-sharing/services/driver-service/internal/index"
	"ride-sharing/services/driver-service/internal/matcher"
	"ride-sharing/services/driver-service/internal/offer"
)

type config struct {
	addr         string
	drivers      int
	rate         int
	workers      int
	queue        int
	offerTimeout time.Duration
	updateEvery  time.Duration
	speed        float64 // meters per second of simulated time (a time-lapse)
	verbose      bool
	webDir       string
}

// Trip states (also sent to the browser as numbers).
const (
	tripSearching = iota
	tripOffered
	tripEnRoute
	tripInRide
	tripDone
	tripCancelled
	tripNoDrivers
)

func isTerminal(s int) bool { return s == tripDone || s == tripCancelled || s == tripNoDrivers }

type simTrip struct {
	id, slug        string
	num             int
	pickup, dropoff point

	// guarded by world.mu
	state         int
	driver        string
	drvNum        int
	cancelPending bool
	finishedAt    time.Time
}

// offerInfo remembers what a simulated driver decided to do with an offer.
type offerInfo struct {
	trip     *simTrip
	decision string // "accept", "decline" or "ignore"
}

type target struct {
	trip  *simTrip
	to    point
	stage int // 0 = driving to the pickup, 1 = driving the rider
}

type agent struct {
	id       string
	num      int
	slug     string
	target   atomic.Pointer[target]
	stop     context.CancelFunc
	stopping atomic.Bool
}

// knobs can be changed live from the browser.
type knobs struct {
	rate, delayMs, drivers, declinePct, ignorePct, cancelPct atomic.Int64
}

type counters struct {
	updates, requests, matched, rejected, accepted, declined atomic.Int64
	timeouts, cancelled, noDrivers, completed                atomic.Int64
}

type world struct {
	cfg    config
	idx    *index.Index
	matcher   *matcher.Matcher
	offers *offer.Manager
	start  time.Time

	knobs  knobs
	c      counters
	lat    latencies
	events eventBuf
	sys    sysStats
	last   atomic.Pointer[stats]

	mu        sync.Mutex // guards the maps below and the guarded fields of simTrip
	trips     map[string]*simTrip
	offerOf   map[string]*offerInfo // driverID -> the offer they currently hold
	agents    map[string]*agent
	nextTrip  int
	nextAgent int
}

// newWorld wires the REAL engine (index, matcher pool, offer manager) to
// simulated drivers and riders.
func newWorld(cfg config) *world {
	w := &world{
		cfg:     cfg,
		idx:     index.New(64, index.DefaultPrecision),
		start:   time.Now(),
		trips:   make(map[string]*simTrip),
		offerOf: make(map[string]*offerInfo),
		agents:  make(map[string]*agent),
	}
	w.knobs.rate.Store(int64(cfg.rate))
	w.knobs.drivers.Store(int64(cfg.drivers))
	w.knobs.delayMs.Store(2)
	w.knobs.declinePct.Store(15)
	w.knobs.ignorePct.Store(15)
	w.knobs.cancelPct.Store(8)

	w.matcher = matcher.New(w.idx, matcher.Config{
		Workers:         cfg.workers,
		QueueSize:       cfg.queue,
		SearchLimit:     5,
		MaxRadiusMeters: 3000,
		// "Search cost" knob: extra simulated work inside every search, so the
		// queue can be shown filling up at demo-sized request rates.
		BeforeSearch: func() {
			if d := w.knobs.delayMs.Load(); d > 0 {
				time.Sleep(time.Duration(d) * time.Millisecond)
			}
		},
	})
	w.offers = offer.New(&simFinder{w}, &simFleet{w}, &simNotifier{w}, offer.Config{
		OfferTimeout: cfg.offerTimeout,
		MatchTimeout: 3 * time.Second,
		MaxAttempts:  5,
	})
	return w
}

func (w *world) run(ctx context.Context, h *hub) {
	go w.superviseDrivers(ctx)
	go w.generate(ctx)
	go w.sampleSystem(ctx)
	go w.frameLoop(ctx, h)
}

func (w *world) close() {
	w.offers.Close()
	w.matcher.Close()
}

func (w *world) emit(kind string, trip, drv int, format string, args ...any) {
	w.events.add(event{
		T:    time.Since(w.start).Milliseconds(),
		K:    kind,
		Trip: trip,
		Drv:  drv,
		M:    fmt.Sprintf(format, args...),
	})
}

// ---- riders ----

// generate sends ride requests at the configured rate.
func (w *world) generate(ctx context.Context) {
	t := time.NewTicker(10 * time.Millisecond)
	defer t.Stop()
	credit := 0.0
	last := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			credit += float64(w.knobs.rate.Load()) * now.Sub(last).Seconds()
			last = now
			if credit > 200 { // don't send a huge burst after a stall
				credit = 200
			}
			for credit >= 1 {
				credit--
				w.newTrip()
			}
		}
	}
}

func (w *world) newTrip() {
	pick := w.pickupPoint()

	w.mu.Lock()
	w.nextTrip++
	n := w.nextTrip
	t := &simTrip{
		id:      "t" + strconv.Itoa(n),
		num:     n,
		slug:    pickSlug(),
		pickup:  pick,
		dropoff: city.dropoffNear(pick),
		drvNum:  -1,
	}
	w.trips[t.id] = t
	w.mu.Unlock()

	w.c.requests.Add(1)
	w.emit("request", n, -1, "Trip %d requested (%s)", n, t.slug)

	w.offers.Start(offer.Trip{
		ID:          t.id,
		RiderID:     "rider-" + t.id,
		PackageSlug: t.slug,
		Lat:         t.pickup.lat,
		Lng:         t.pickup.lng,
		Payload:     []byte(t.id),
	})

	if int64(rand.IntN(100)) < w.knobs.cancelPct.Load() {
		delay := time.Duration(1500+rand.IntN(7000)) * time.Millisecond
		time.AfterFunc(delay, func() { w.riderCancels(t) })
	}
}

func (w *world) pickupPoint() point {
	if rand.IntN(100) < 70 {
		return city.hotspot()
	}
	return city.uniform()
}

func pickSlug() string {
	switch n := rand.IntN(10); {
	case n < 6:
		return "sedan"
	case n < 9:
		return "suv"
	}
	return "van"
}

// riderCancels is the rider pressing "cancel" some seconds after requesting.
func (w *world) riderCancels(t *simTrip) {
	w.mu.Lock()
	state := t.state
	if state != tripSearching && state != tripOffered && state != tripEnRoute {
		w.mu.Unlock() // already finished, or already in the ride: too late to cancel
		return
	}
	if state != tripEnRoute {
		t.cancelPending = true
	}
	w.mu.Unlock()

	if state == tripEnRoute { // a driver is already driving to the pickup
		w.cancelEnRoute(t)
		return
	}

	// Still searching or waiting for an answer: stop the offer goroutine.
	// That releases the offered driver and sends no further offers.
	if w.offers.Cancel(t.id) {
		w.mu.Lock()
		st := t.state
		w.mu.Unlock()
		if st == tripEnRoute { // a driver was assigned a moment before the cancel
			w.cancelEnRoute(t)
			return
		}
		w.cancelTrip(t, "rider cancelled")
		return
	}

	w.mu.Lock()
	t.cancelPending = false
	st := t.state
	w.mu.Unlock()
	if st == tripEnRoute {
		w.cancelEnRoute(t)
	}
}

// cancelEnRoute frees a driver who was already driving to the pickup.
func (w *world) cancelEnRoute(t *simTrip) {
	w.mu.Lock()
	if t.state != tripEnRoute {
		w.mu.Unlock()
		return
	}
	t.state = tripCancelled // first, so a driver arriving right now stops
	drv := t.driver
	a := w.agents[drv]
	w.mu.Unlock()

	if a != nil {
		a.target.Store(nil)
	}
	w.idx.Complete(drv) // assigned -> available
	w.c.cancelled.Add(1)
	w.emit("cancelled", t.num, -1, "Trip %d cancelled by the rider (driver freed)", t.num)
	w.finishTrip(t)
}

// cancelTrip marks a trip cancelled unless it already finished.
func (w *world) cancelTrip(t *simTrip, reason string) {
	w.mu.Lock()
	if isTerminal(t.state) {
		w.mu.Unlock()
		return
	}
	t.state = tripCancelled
	w.mu.Unlock()

	w.c.cancelled.Add(1)
	w.emit("cancelled", t.num, -1, "Trip %d cancelled (%s)", t.num, reason)
	w.finishTrip(t)
}

// finishTrip keeps a finished trip visible for a moment, then forgets it.
func (w *world) finishTrip(t *simTrip) {
	w.mu.Lock()
	if t.finishedAt.IsZero() {
		t.finishedAt = time.Now()
	}
	w.mu.Unlock()
	time.AfterFunc(2*time.Second, func() {
		w.mu.Lock()
		delete(w.trips, t.id)
		w.mu.Unlock()
	})
}

// decide is a simulated driver's reaction to an offer.
func (w *world) decide() string {
	n := int64(rand.IntN(100))
	ignore := w.knobs.ignorePct.Load()
	decline := w.knobs.declinePct.Load()
	switch {
	case n < ignore:
		return "ignore"
	case n < ignore+decline:
		return "decline"
	}
	return "accept"
}

// ---- drivers ----

// superviseDrivers adds or removes driver goroutines to match the knob.
func (w *world) superviseDrivers(ctx context.Context) {
	t := time.NewTicker(200 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			want := int(w.knobs.drivers.Load())
			w.mu.Lock()
			have := len(w.agents)
			w.mu.Unlock()
			for i := 0; i < 40 && have < want; i++ {
				w.spawnAgent(ctx)
				have++
			}
			for i := 0; i < 40 && have > want; i++ {
				w.stopOneAgent()
				have--
			}
		}
	}
}

func (w *world) spawnAgent(ctx context.Context) {
	cctx, cancel := context.WithCancel(ctx)
	w.mu.Lock()
	w.nextAgent++
	a := &agent{num: w.nextAgent, slug: pickSlug(), stop: cancel}
	a.id = "driver-" + strconv.Itoa(a.num)
	w.agents[a.id] = a
	w.mu.Unlock()
	go w.runAgent(cctx, a)
}

// stopOneAgent takes a driver offline, preferring one that has no trip.
func (w *world) stopOneAgent() {
	w.mu.Lock()
	var pick *agent
	for _, a := range w.agents {
		if a.stopping.Load() {
			continue
		}
		if a.target.Load() == nil {
			pick = a
			break
		}
		if pick == nil {
			pick = a
		}
	}
	if pick != nil {
		pick.stopping.Store(true)
	}
	w.mu.Unlock()
	if pick != nil {
		pick.stop()
	}
}