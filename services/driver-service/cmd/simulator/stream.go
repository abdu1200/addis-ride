package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"ride-sharing/services/driver-service/internal/index"
)

// ---- events ----

type event struct {
	T    int64  `json:"t"`    // ms since the simulator started
	K    string `json:"k"`    // kind: request, matched, offer, declined, timeout, assigned, ...
	Trip int    `json:"trip"` // trip number, 0 if none
	Drv  int    `json:"drv"`  // driver number, -1 if none
	M    string `json:"m"`    // human-readable text
}

type eventBuf struct {
	mu      sync.Mutex
	evs     []event
	dropped int64
}

func (b *eventBuf) add(e event) {
	b.mu.Lock()
	if len(b.evs) < 200 { // cap per frame: the browser can't read more anyway
		b.evs = append(b.evs, e)
	} else {
		b.dropped++
	}
	b.mu.Unlock()
}

func (b *eventBuf) drain() ([]event, int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := b.evs
	b.evs = make([]event, 0, 64)
	return out, b.dropped
}

// ---- latency windows ----

type sample struct {
	at int64
	d  time.Duration
}

// ring keeps the most recent samples; percentiles only look at the last few seconds.
type ring struct {
	mu  sync.Mutex
	buf [4096]sample
	n   int
	pos int
}

func (r *ring) add(d time.Duration) {
	r.mu.Lock()
	r.buf[r.pos] = sample{at: time.Now().UnixNano(), d: d}
	r.pos = (r.pos + 1) % len(r.buf)
	if r.n < len(r.buf) {
		r.n++
	}
	r.mu.Unlock()
}

// pcts returns percentiles in microseconds over the last `window`.
func (r *ring) pcts(window time.Duration, ps ...float64) []float64 {
	cutoff := time.Now().Add(-window).UnixNano()
	r.mu.Lock()
	ds := make([]time.Duration, 0, r.n)
	for i := 0; i < r.n; i++ {
		if s := r.buf[i]; s.at >= cutoff {
			ds = append(ds, s.d)
		}
	}
	r.mu.Unlock()

	out := make([]float64, len(ps))
	if len(ds) == 0 {
		return out
	}
	slices.Sort(ds)
	for i, p := range ps {
		k := int(math.Ceil(p/100*float64(len(ds)))) - 1
		out[i] = float64(ds[max(k, 0)].Nanoseconds()) / 1000
	}
	return out
}

type latencies struct {
	match ring // trip search: submit -> result (includes waiting in the queue)
	qwait ring // time spent waiting in the queue
	upd   ring // one location update (UpdateLocation)
}

// ---- memory / GC, sampled once a second ----

type sysStats struct {
	mu         sync.Mutex
	heapMB     float64
	numGC      uint32
	maxPauseUs float64
	goroutines int
}

func (w *world) sampleSystem(ctx context.Context) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			var ms runtime.MemStats
			runtime.ReadMemStats(&ms)
			var maxPause uint64
			n := min(ms.NumGC, 256)
			for i := uint32(0); i < n; i++ {
				maxPause = max(maxPause, ms.PauseNs[i])
			}
			w.sys.mu.Lock()
			w.sys.heapMB = float64(ms.HeapAlloc) / (1 << 20)
			w.sys.numGC = ms.NumGC
			w.sys.maxPauseUs = float64(maxPause) / 1000
			w.sys.goroutines = runtime.NumGoroutine()
			w.sys.mu.Unlock()
		}
	}
}

// ---- frames ----

type knobsView struct {
	Rate       int64 `json:"rate"`
	DelayMs    int64 `json:"delay_ms"`
	Drivers    int64 `json:"drivers"`
	DeclinePct int64 `json:"decline_pct"`
	IgnorePct  int64 `json:"ignore_pct"`
	CancelPct  int64 `json:"cancel_pct"`
}

func (w *world) knobsView() knobsView {
	return knobsView{
		Rate:       w.knobs.rate.Load(),
		DelayMs:    w.knobs.delayMs.Load(),
		Drivers:    w.knobs.drivers.Load(),
		DeclinePct: w.knobs.declinePct.Load(),
		IgnorePct:  w.knobs.ignorePct.Load(),
		CancelPct:  w.knobs.cancelPct.Load(),
	}
}

type stats struct {
	Drivers     int `json:"drivers"`
	Available   int `json:"available"`
	Reserved    int `json:"reserved"`
	EnRoute     int `json:"en_route"`
	InRide      int `json:"in_ride"`
	ActiveTrips int `json:"active_trips"`

	UpdatesPerSec  float64 `json:"updates_per_sec"`
	RequestsPerSec float64 `json:"requests_per_sec"`
	MatchesPerSec  float64 `json:"matches_per_sec"`
	RejectedPerSec float64 `json:"rejected_per_sec"`

	Requests  int64 `json:"requests"`
	Updates   int64 `json:"updates"`
	Matched   int64 `json:"matched"`
	Rejected  int64 `json:"rejected"`
	Accepted  int64 `json:"accepted"`
	Declined  int64 `json:"declined"`
	Timeouts  int64 `json:"timeouts"`
	Cancelled int64 `json:"cancelled"`
	NoDrivers int64 `json:"no_drivers"`
	Completed int64 `json:"completed"`

	QueueLen int `json:"queue_len"`
	QueueCap int `json:"queue_cap"`
	Workers  int `json:"workers"`

	MatchP50us     float64 `json:"match_p50_us"`
	MatchP95us     float64 `json:"match_p95_us"`
	MatchP99us     float64 `json:"match_p99_us"`
	QueueWaitP99us float64 `json:"queue_wait_p99_us"`
	UpdateP50us    float64 `json:"update_p50_us"`
	UpdateP99us    float64 `json:"update_p99_us"`

	HeapMB        float64   `json:"heap_mb"`
	Goroutines    int       `json:"goroutines"`
	NumGC         uint32    `json:"num_gc"`
	MaxGCPauseUs  float64   `json:"max_gc_pause_us"`
	EventsDropped int64     `json:"events_dropped"`
	Knobs         knobsView `json:"knobs"`
}

// frame is what the browser receives about 10 times a second.
//
//	d  : 4 numbers per driver  -> id, x, y, state   (0 available, 1 reserved, 2 en route, 3 in ride)
//	tr : 8 numbers per trip    -> num, x, y, state, driver, ageMs, destX, destY
//	     trip state: 0 searching, 1 offered, 2 en route, 3 in ride, 4 done, 5 cancelled, 6 no drivers
//	x, y are 0..10000 across the map (y = 0 is north)
type frame struct {
	T  int64   `json:"t"`
	D  []int   `json:"d"`
	Tr []int   `json:"tr"`
	Ev []event `json:"ev,omitempty"`
	St stats   `json:"st"`
}

type counterSnap struct {
	at                                  time.Time
	updates, requests, matched, rejected int64
}

func (c *counters) snap(at time.Time) counterSnap {
	return counterSnap{
		at:       at,
		updates:  c.updates.Load(),
		requests: c.requests.Load(),
		matched:  c.matched.Load(),
		rejected: c.rejected.Load(),
	}
}

func (w *world) buildFrame(now time.Time, cur, old counterSnap) frame {
	dt := cur.at.Sub(old.at).Seconds()
	if dt <= 0 {
		dt = 1
	}

	var st stats

	// Drivers: read straight from the real index.
	w.mu.Lock()
	list := make([]*agent, 0, len(w.agents))
	for _, a := range w.agents {
		list = append(list, a)
	}
	w.mu.Unlock()

	d := make([]int, 0, len(list)*4)
	for _, a := range list {
		dr, ok := w.idx.Get(a.id)
		if !ok {
			continue
		}
		x, y := city.norm(point{dr.Lat, dr.Lng})
		s := 0
		switch dr.State {
		case index.StateReserved:
			s = 1
			st.Reserved++
		case index.StateAssigned:
			if tg := a.target.Load(); tg != nil && tg.stage == 1 {
				s = 3
				st.InRide++
			} else {
				s = 2
				st.EnRoute++
			}
		default:
			st.Available++
		}
		st.Drivers++
		d = append(d, a.num, x, y, s)
	}

	// Trips: active ones, plus finished ones for a moment so the browser can flash them.
	tr := make([]int, 0, 8*64)
	w.mu.Lock()
	for _, t := range w.trips {
		age := 0
		if isTerminal(t.state) {
			if !t.finishedAt.IsZero() {
				age = int(now.Sub(t.finishedAt).Milliseconds())
			}
			if age > 1800 {
				continue
			}
		} else {
			st.ActiveTrips++
		}
		x, y := city.norm(t.pickup)
		dx, dy := city.norm(t.dropoff)
		tr = append(tr, t.num, x, y, t.state, t.drvNum, age, dx, dy)
		if len(tr) >= 8*600 {
			break
		}
	}
	w.mu.Unlock()

	evs, dropped := w.events.drain()

	// Rates over about the last second.
	st.UpdatesPerSec = float64(cur.updates-old.updates) / dt
	st.RequestsPerSec = float64(cur.requests-old.requests) / dt
	st.MatchesPerSec = float64(cur.matched-old.matched) / dt
	st.RejectedPerSec = float64(cur.rejected-old.rejected) / dt

	st.Requests = w.c.requests.Load()
	st.Updates = w.c.updates.Load()
	st.Matched = w.c.matched.Load()
	st.Rejected = w.c.rejected.Load()
	st.Accepted = w.c.accepted.Load()
	st.Declined = w.c.declined.Load()
	st.Timeouts = w.c.timeouts.Load()
	st.Cancelled = w.c.cancelled.Load()
	st.NoDrivers = w.c.noDrivers.Load()
	st.Completed = w.c.completed.Load()

	st.QueueLen = w.matcher.Stats().QueueLen
	st.QueueCap = w.cfg.queue
	st.Workers = w.cfg.workers

	window := 5 * time.Second
	mp := w.lat.match.pcts(window, 50, 95, 99)
	st.MatchP50us, st.MatchP95us, st.MatchP99us = mp[0], mp[1], mp[2]
	st.QueueWaitP99us = w.lat.qwait.pcts(window, 99)[0]
	up := w.lat.upd.pcts(window, 50, 99)
	st.UpdateP50us, st.UpdateP99us = up[0], up[1]

	w.sys.mu.Lock()
	st.HeapMB = w.sys.heapMB
	st.Goroutines = w.sys.goroutines
	st.NumGC = w.sys.numGC
	st.MaxGCPauseUs = w.sys.maxPauseUs
	w.sys.mu.Unlock()

	st.EventsDropped = dropped
	st.Knobs = w.knobsView()

	return frame{T: now.Sub(w.start).Milliseconds(), D: d, Tr: tr, Ev: evs, St: st}
}

func (w *world) frameLoop(ctx context.Context, h *hub) {
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	hist := make([]counterSnap, 0, 12)
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			cur := w.c.snap(now)
			hist = append(hist, cur)
			if len(hist) > 11 {
				hist = hist[1:]
			}
			f := w.buildFrame(now, cur, hist[0])
			st := f.St
			w.last.Store(&st)

			if h.count() == 0 {
				continue // nobody is watching: skip the JSON work
			}
			if b, err := json.Marshal(f); err == nil {
				h.publish(b)
			}
		}
	}
}

// ---- fan-out to browsers ----

type hub struct {
	mu   sync.Mutex
	subs map[chan []byte]struct{}
}

func newHub() *hub { return &hub{subs: make(map[chan []byte]struct{})} }

func (h *hub) subscribe() chan []byte {
	ch := make(chan []byte, 8)
	h.mu.Lock()
	h.subs[ch] = struct{}{}
	h.mu.Unlock()
	return ch
}

func (h *hub) unsubscribe(ch chan []byte) {
	h.mu.Lock()
	delete(h.subs, ch)
	h.mu.Unlock()
}

func (h *hub) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.subs)
}

// publish never blocks: a slow browser just misses frames.
func (h *hub) publish(b []byte) {
	h.mu.Lock()
	for ch := range h.subs {
		select {
		case ch <- b:
		default:
		}
	}
	h.mu.Unlock()
}

// ---- HTTP ----

func cors(rw http.ResponseWriter) {
	h := rw.Header()
	h.Set("Access-Control-Allow-Origin", "*")
	h.Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	h.Set("Access-Control-Allow-Headers", "Content-Type")
}

func (w *world) hello() []byte {
	b, _ := json.Marshal(map[string]any{
		"region": map[string]float64{
			"min_lat": city.minLat, "max_lat": city.maxLat,
			"min_lng": city.minLng, "max_lng": city.maxLng,
		},
		"width_m":           city.widthMeters(),
		"height_m":          city.heightMeters(),
		"workers":           w.cfg.workers,
		"queue_cap":         w.cfg.queue,
		"offer_timeout_sec": w.cfg.offerTimeout.Seconds(),
		"update_every_sec":  w.cfg.updateEvery.Seconds(),
		"speed_mps":         w.cfg.speed,
		"driver_states":     []string{"available", "reserved", "en_route", "in_ride"},
		"trip_states":       []string{"searching", "offered", "en_route", "in_ride", "done", "cancelled", "no_drivers"},
		"coords":            "x and y are 0..10000 across the map, y = 0 is north",
	})
	return b
}

func (w *world) handleStream(h *hub) http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		fl, ok := rw.(http.Flusher)
		if !ok {
			http.Error(rw, "streaming not supported", http.StatusInternalServerError)
			return
		}
		cors(rw)
		rw.Header().Set("Content-Type", "text/event-stream")
		rw.Header().Set("Cache-Control", "no-cache")
		rw.Header().Set("Connection", "keep-alive")
		rw.Header().Set("X-Accel-Buffering", "no")

		ch := h.subscribe()
		defer h.unsubscribe(ch)

		fmt.Fprintf(rw, "event: hello\ndata: %s\n\n", w.hello())
		fl.Flush()

		for {
			select {
			case <-r.Context().Done():
				return
			case b := <-ch:
				fmt.Fprintf(rw, "data: %s\n\n", b)
				fl.Flush()
			}
		}
	}
}

func (w *world) handleStats(rw http.ResponseWriter, r *http.Request) {
	cors(rw)
	s := w.last.Load()
	if s == nil {
		http.Error(rw, "no data yet", http.StatusServiceUnavailable)
		return
	}
	rw.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(rw).Encode(s)
}

func (w *world) handleControl(rw http.ResponseWriter, r *http.Request) {
	cors(rw)
	switch r.Method {
	case http.MethodOptions:
		rw.WriteHeader(http.StatusNoContent)
		return
	case http.MethodPost:
		var req struct {
			Rate       *int `json:"rate"`
			DelayMs    *int `json:"delay_ms"`
			Drivers    *int `json:"drivers"`
			DeclinePct *int `json:"decline_pct"`
			IgnorePct  *int `json:"ignore_pct"`
			CancelPct  *int `json:"cancel_pct"`
		}
		if err := json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&req); err != nil {
			http.Error(rw, "bad json", http.StatusBadRequest)
			return
		}
		set := func(k *atomic.Int64, v *int, lo, hi int) {
			if v != nil {
				k.Store(int64(min(max(*v, lo), hi)))
			}
		}
		set(&w.knobs.rate, req.Rate, 0, 2000)
		set(&w.knobs.delayMs, req.DelayMs, 0, 200)
		set(&w.knobs.drivers, req.Drivers, 0, 2000)
		set(&w.knobs.declinePct, req.DeclinePct, 0, 100)
		set(&w.knobs.ignorePct, req.IgnorePct, 0, 100)
		set(&w.knobs.cancelPct, req.CancelPct, 0, 100)
	case http.MethodGet:
	default:
		http.Error(rw, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	rw.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(rw).Encode(w.knobsView())
}