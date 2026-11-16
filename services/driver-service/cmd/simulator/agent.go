package main

import (
	"context"
	"math"
	"math/rand/v2"
	"time"
)

// runAgent is one simulated driver: it registers in the real index, then sends
// a location update every cfg.updateEvery, wandering around when idle and
// driving to the pickup and the destination when it has a trip.
func (w *world) runAgent(ctx context.Context, a *agent) {
	defer func() {
		w.idx.Remove(a.id) // first: from now on nobody can reserve this driver
		w.mu.Lock()
		delete(w.agents, a.id)
		w.mu.Unlock()
		if tg := a.target.Load(); tg != nil {
			w.cancelTrip(tg.trip, "its driver went offline")
		}
	}()

	pos := city.uniform()
	if rand.IntN(100) < 50 {
		pos = city.hotspot()
	}
	if err := w.idx.Upsert(a.id, a.slug, pos.lat, pos.lng); err != nil {
		return
	}
	heading := rand.Float64() * 2 * math.Pi

	// Spread the drivers' updates out instead of all firing together.
	if !sleepCtx(ctx, time.Duration(rand.Int64N(int64(w.cfg.updateEvery)))) {
		return
	}
	last := time.Now()
	tick := time.NewTicker(w.cfg.updateEvery)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-tick.C:
			step := w.cfg.speed * now.Sub(last).Seconds()
			last = now

			if tg := a.target.Load(); tg != nil {
				var arrived bool
				pos, arrived = moveToward(pos, tg.to, step)
				if arrived {
					w.onArrive(a, tg)
				}
			} else {
				heading += (rand.Float64() - 0.5) * 0.9
				pos, heading = city.wander(pos, heading, step*0.35)
			}

			t0 := time.Now()
			ok, _ := w.idx.UpdateLocation(a.id, pos.lat, pos.lng)
			w.lat.upd.add(time.Since(t0))
			w.c.updates.Add(1)
			if !ok {
				return // the index no longer knows this driver
			}
		}
	}
}

// onArrive runs when a driver reaches the pickup (stage 0) or the destination (stage 1).
func (w *world) onArrive(a *agent, tg *target) {
	t := tg.trip

	if tg.stage == 0 {
		w.mu.Lock()
		if t.state != tripEnRoute { // cancelled while driving
			w.mu.Unlock()
			return
		}
		t.state = tripInRide
		w.mu.Unlock()

		a.target.Store(&target{trip: t, to: t.dropoff, stage: 1})
		w.emit("pickup", t.num, a.num, "Driver %d picked up the rider of trip %d", a.num, t.num)
		return
	}

	w.mu.Lock()
	if t.state != tripInRide {
		w.mu.Unlock()
		return
	}
	t.state = tripDone
	w.mu.Unlock()

	a.target.Store(nil)
	w.idx.Complete(a.id) // assigned -> available
	w.c.completed.Add(1)
	w.emit("completed", t.num, a.num, "Trip %d completed", t.num)
	w.finishTrip(t)
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}