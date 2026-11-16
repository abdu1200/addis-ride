package main

import (
	"context"
	"errors"
	"math/rand/v2"
	"strconv"
	"strings"
	"time"

	"ride-sharing/services/driver-service/internal/matcher"
)

// These three types plug the REAL offer manager into the simulation:
//
//	simFinder   = the matcher pool, plus timing and events around it
//	simFleet    = the real index state changes, plus moving the simulated car
//	simNotifier = "RabbitMQ": sends the offer to a simulated driver who then
//	              accepts, declines or ignores it

type simFinder struct{ w *world }
type simFleet struct{ w *world }
type simNotifier struct{ w *world }

func numAfter(prefix, id string) int {
	n, err := strconv.Atoi(strings.TrimPrefix(id, prefix))
	if err != nil {
		return -1
	}
	return n
}

// ---- finder ----

func (f *simFinder) Match(ctx context.Context, req matcher.Request) (matcher.Result, error) {
	w := f.w
	start := time.Now()
	res, err := w.matcher.Match(ctx, req)
	tripNum := numAfter("t", req.TripID)

	switch {
	case errors.Is(err, matcher.ErrQueueFull):
		w.c.rejected.Add(1)
		w.emit("rejected", tripNum, -1, "Queue full: search for trip %d rejected", tripNum)
	case err == nil:
		w.lat.match.add(time.Since(start)) // submit -> result, includes the queue wait
		w.lat.qwait.add(res.QueueWait)
		if res.Matched {
			w.c.matched.Add(1)
			drv := numAfter("driver-", res.DriverID)
			w.emit("matched", tripNum, drv, "Trip %d reserved driver %d (%.0f m away)", tripNum, drv, res.DistanceMeters)
		}
	}
	return res, err
}

// ---- fleet ----

func (f *simFleet) Assign(driverID string) bool {
	w := f.w
	ok := w.idx.Assign(driverID) // the real reserved -> assigned flip

	w.mu.Lock()
	info := w.offerOf[driverID]
	delete(w.offerOf, driverID)
	a := w.agents[driverID]
	w.mu.Unlock()

	if !ok {
		return false
	}
	if info == nil {
		return true
	}
	t := info.trip

	w.mu.Lock()
	cancelled := t.cancelPending || isTerminal(t.state)
	if !cancelled && a != nil {
		t.state = tripEnRoute
		t.driver = driverID
		t.drvNum = a.num
	}
	w.mu.Unlock()

	if cancelled || a == nil {
		// The rider cancelled just as the driver accepted (or the driver
		// vanished): give the driver straight back.
		w.idx.Complete(driverID)
		reason := "the rider cancelled"
		if a == nil {
			reason = "its driver went offline"
		}
		w.cancelTrip(t, reason)
		return true
	}

	a.target.Store(&target{trip: t, to: t.pickup, stage: 0}) // start driving
	w.c.accepted.Add(1)
	w.emit("assigned", t.num, a.num, "Driver %d accepted trip %d", a.num, t.num)
	return true
}

func (f *simFleet) Release(driverID string) bool {
	w := f.w
	ok := w.idx.Release(driverID) // the real reserved -> available flip

	w.mu.Lock()
	info := w.offerOf[driverID]
	delete(w.offerOf, driverID)
	silent := false
	var t *simTrip
	if info != nil {
		t = info.trip
		silent = info.decision == "ignore" && !t.cancelPending
		if t.state == tripOffered {
			t.state = tripSearching
		}
		t.driver = ""
		t.drvNum = -1
	}
	w.mu.Unlock()

	if silent {
		w.c.timeouts.Add(1)
		drv := numAfter("driver-", driverID)
		w.emit("timeout", t.num, drv, "Driver %d didn't answer trip %d: next driver", drv, t.num)
	}
	return ok
}

// ---- notifier ----

func (n *simNotifier) SendOffer(_ context.Context, driverID string, payload []byte) error {
	w := n.w
	tripID := string(payload)
	decision := w.decide()
	drv := numAfter("driver-", driverID)

	w.mu.Lock()
	t := w.trips[tripID]
	if t == nil || isTerminal(t.state) {
		w.mu.Unlock()
		return nil
	}
	w.offerOf[driverID] = &offerInfo{trip: t, decision: decision}
	t.state = tripOffered
	t.driver = driverID
	t.drvNum = drv
	w.mu.Unlock()

	w.emit("offer", t.num, drv, "Offer sent: trip %d to driver %d", t.num, drv)

	// The simulated driver "thinks" for a moment, then answers (or never does).
	think := time.Duration(800+rand.IntN(2700)) * time.Millisecond
	switch decision {
	case "accept":
		time.AfterFunc(think, func() { w.offers.OnAccept(tripID, driverID) })
	case "decline":
		time.AfterFunc(think, func() {
			w.c.declined.Add(1)
			w.emit("declined", t.num, drv, "Driver %d declined trip %d", drv, t.num)
			w.offers.OnDecline(tripID, driverID)
		})
	} // "ignore": say nothing, the offer manager's timer fires
	return nil
}

func (n *simNotifier) NoDriversFound(_ context.Context, riderID string) error {
	w := n.w
	id := strings.TrimPrefix(riderID, "rider-")

	w.mu.Lock()
	t := w.trips[id]
	if t == nil || isTerminal(t.state) {
		w.mu.Unlock()
		return nil
	}
	t.state = tripNoDrivers
	w.mu.Unlock()

	w.c.noDrivers.Add(1)
	w.emit("no_drivers", t.num, -1, "Trip %d: no drivers found", t.num)
	w.finishTrip(t)
	return nil
}