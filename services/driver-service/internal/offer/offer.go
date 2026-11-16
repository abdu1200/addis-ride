// Package offer runs the life of a trip offer:
//
//	find a driver -> offer the trip -> wait for accept / decline / timeout / cancel
//	      ^                                        |
//	      +---- next driver (never the same one) <-+  (decline or timeout)
//
// Each trip gets ONE goroutine that owns all of that trip's state. Everything
// else (a driver's accept, a decline, a rider cancel) talks to it through a
// channel or a context. Because only that goroutine makes decisions, an accept
// and a timeout arriving at the same moment can never both win: the goroutine's
// select takes one of them, then the other is simply ignored.
package offer

import (
	"context"
	"log"
	"sync"
	"time"

	"ride-sharing/services/driver-service/internal/matcher"
)

// Finder finds and RESERVES the nearest available driver. *matcher.Pool does this.
type Finder interface {
	Match(ctx context.Context, req matcher.Request) (matcher.Result, error)
}

// Fleet changes driver states. *index.Index does this.
type Fleet interface {
	Assign(driverID string) bool  // reserved -> assigned
	Release(driverID string) bool // reserved -> available
}

// Notifier sends messages out (RabbitMQ in the real service, a fake in tests).
type Notifier interface {
	SendOffer(ctx context.Context, driverID string, payload []byte) error
	NoDriversFound(ctx context.Context, riderID string) error
}

type Config struct {
	OfferTimeout time.Duration // how long a driver has to answer (0 = 15s)
	MatchTimeout time.Duration // how long one driver search may take (0 = 3s)
	MaxAttempts  int           // how many drivers to try per trip (0 = 5)
}

// Trip is what the manager needs to find drivers for one trip.
type Trip struct {
	ID          string
	RiderID     string
	PackageSlug string
	Lat, Lng    float64 // pickup point
	Payload     []byte  // sent untouched to the driver as the offer
}

type answer struct {
	driverID string
	accepted bool
}

type tripState struct {
	trip    Trip
	answers chan answer
	cancel  context.CancelFunc
}

type outcome int

const (
	outcomeAccepted outcome = iota
	outcomeDeclined
	outcomeTimedOut
	outcomeCancelled
)

type Manager struct {
	cfg    Config
	finder Finder // matcher
	fleet  Fleet  // index
	notify Notifier

	baseCtx context.Context
	stop    context.CancelFunc

	mu     sync.Mutex
	trips  map[string]*tripState
	closed bool
	wg     sync.WaitGroup
}

func New(f Finder, fl Fleet, n Notifier, cfg Config) *Manager {
	if cfg.OfferTimeout <= 0 {
		cfg.OfferTimeout = 15 * time.Second
	}
	if cfg.MatchTimeout <= 0 {
		cfg.MatchTimeout = 3 * time.Second
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 5
	}
	ctx, stop := context.WithCancel(context.Background())
	return &Manager{
		cfg: cfg, finder: f, fleet: fl, notify: n,
		baseCtx: ctx, stop: stop,
		trips: make(map[string]*tripState),
	}
}

// Start begins finding a driver for the trip and returns immediately.
// It returns false if the trip is already running (a redelivered message)
// or the manager is closed.
func (m *Manager) Start(t Trip) bool {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return false
	}
	if _, running := m.trips[t.ID]; running {
		m.mu.Unlock()
		return false
	}
	ctx, cancel := context.WithCancel(m.baseCtx)
	ts := &tripState{trip: t, answers: make(chan answer, 4), cancel: cancel}
	m.trips[t.ID] = ts
	m.wg.Add(1) // under the lock, so Close can't miss this goroutine
	m.mu.Unlock()

	go m.run(ctx, ts)
	return true
}

// OnAccept tells the trip's goroutine that a driver accepted.
// It returns false if the trip is unknown or already finished.
func (m *Manager) OnAccept(tripID, driverID string) bool {
	return m.deliver(tripID, answer{driverID: driverID, accepted: true})
}

// OnDecline tells the trip's goroutine that a driver declined.
func (m *Manager) OnDecline(tripID, driverID string) bool {
	return m.deliver(tripID, answer{driverID: driverID, accepted: false})
}

func (m *Manager) deliver(tripID string, a answer) bool {
	m.mu.Lock()
	ts := m.trips[tripID]
	m.mu.Unlock()
	if ts == nil {
		return false
	}
	select {
	case ts.answers <- a:
		return true
	default:
		return false // buffer full: only possible with a flood of stale answers
	}
}

// Cancel stops looking for a driver for this trip (the rider cancelled).
// A driver that was only offered the trip is released. It returns false if the
// trip is unknown or already finished, for example because a driver already
// accepted: that driver is Assigned and is freed later with CompleteTrip.
func (m *Manager) Cancel(tripID string) bool {
	m.mu.Lock()
	ts := m.trips[tripID]
	m.mu.Unlock()
	if ts == nil {
		return false
	}
	ts.cancel()
	return true
}

// Active returns the number of trips still looking for a driver.
func (m *Manager) Active() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.trips)
}

// Close cancels every running trip (releasing its reserved driver) and waits
// for all trip goroutines to exit.
func (m *Manager) Close() {
	m.mu.Lock()
	m.closed = true
	m.mu.Unlock()
	m.stop()
	m.wg.Wait()
}

// run is the trip's goroutine. It is the only code that decides what happens
// to this trip.
func (m *Manager) run(ctx context.Context, ts *tripState) {
	defer m.wg.Done()
	defer m.finish(ts)

	excluded := map[string]struct{}{} // drivers that already declined or timed out

	for attempt := 0; attempt < m.cfg.MaxAttempts; attempt++ {
		driverID, found, err := m.findDriver(ctx, ts.trip, excluded)

		if ctx.Err() != nil { // rider cancelled or the service is shutting down
			if found {
				m.fleet.Release(driverID)
			}
			return
		}
		if err != nil { // overloaded or timed out: wait a little and try again
			log.Printf("offer: trip %s: matching failed: %v", ts.trip.ID, err)
			if !sleep(ctx, 200*time.Millisecond) {
				return
			}
			continue
		}
		if !found { // nobody left to ask
			break
		}

		if err := m.notify.SendOffer(ctx, driverID, ts.trip.Payload); err != nil {
			log.Printf("offer: trip %s: sending offer to %s failed: %v", ts.trip.ID, driverID, err)
			m.fleet.Release(driverID)
			excluded[driverID] = struct{}{}
			continue
		}

		switch m.waitForAnswer(ctx, ts, driverID) {
		case outcomeAccepted:
			if m.fleet.Assign(driverID) {
				return // done: the driver has the trip
			}
			// The driver went offline right as they accepted. Try another one.
			log.Printf("offer: trip %s: driver %s accepted but could not be assigned", ts.trip.ID, driverID)
			excluded[driverID] = struct{}{}
		case outcomeCancelled:
			m.fleet.Release(driverID)
			return
		default: // declined or timed out
			m.fleet.Release(driverID)
			excluded[driverID] = struct{}{}
		}
	}

	// Ran out of drivers or attempts.
	nctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := m.notify.NoDriversFound(nctx, ts.trip.RiderID); err != nil {
		log.Printf("offer: trip %s: publishing no-drivers-found failed: %v", ts.trip.ID, err)
	}
}

// waitForAnswer waits for the offered driver to answer. First event wins.
func (m *Manager) waitForAnswer(ctx context.Context, ts *tripState, driverID string) outcome {
	timer := time.NewTimer(m.cfg.OfferTimeout)
	defer timer.Stop()

	for {
		select {
		case a := <-ts.answers:
			if a.driverID != driverID {
				continue // a late answer from an earlier driver: ignore it
			}
			if a.accepted {
				return outcomeAccepted
			}
			return outcomeDeclined
		case <-timer.C:
			return outcomeTimedOut
		case <-ctx.Done():
			return outcomeCancelled
		}
	}
}

// findDriver reserves the nearest driver that isn't in `excluded`.
func (m *Manager) findDriver(ctx context.Context, t Trip, excluded map[string]struct{}) (string, bool, error) {
	mctx, cancel := context.WithTimeout(ctx, m.cfg.MatchTimeout)
	defer cancel()

	// Matcher workers read this map while we keep changing ours, so give them a copy.
	ex := make(map[string]struct{}, len(excluded))
	for id := range excluded {
		ex[id] = struct{}{}
	}

	res, err := m.finder.Match(mctx, matcher.Request{
		TripID:      t.ID,
		PackageSlug: t.PackageSlug,
		Lat:         t.Lat,
		Lng:         t.Lng,
		Exclude:     ex,
	})
	if err != nil {
		return "", false, err
	}
	if !res.Matched {
		return "", false, nil
	}
	return res.DriverID, true, nil
}

func (m *Manager) finish(ts *tripState) {
	ts.cancel() // frees the context's resources
	m.mu.Lock()
	if m.trips[ts.trip.ID] == ts {
		delete(m.trips, ts.trip.ID)
	}
	m.mu.Unlock()
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