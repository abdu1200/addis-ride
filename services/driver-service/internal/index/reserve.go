package index

// transition atomically changes a driver's state from `from` to `to`.
// It returns true only if the driver was in state `from` at that exact moment.
//
// The driver-shard read lock is held only to find the atomic. The
// compare-and-swap itself takes no lock, so many requests racing for the same
// driver are decided by the CPU: exactly one CAS succeeds, all others fail.
func (idx *Index) transition(id string, from, to State) bool {
	ds := idx.driverShardFor(id)
	ds.mu.RLock()
	st := ds.states[id]
	ds.mu.RUnlock()
	if st == nil {
		return false // unknown or removed driver
	}
	return st.CompareAndSwap(int32(from), int32(to))
}

// Reserve: Available -> Reserved. Exactly one caller wins per driver.
func (idx *Index) Reserve(id string) bool { return idx.transition(id, StateAvailable, StateReserved) }

// Assign: Reserved -> Assigned (the driver accepted the trip).
func (idx *Index) Assign(id string) bool { return idx.transition(id, StateReserved, StateAssigned) }

// Release: Reserved -> Available (the driver declined, timed out, or the offer failed).
func (idx *Index) Release(id string) bool { return idx.transition(id, StateReserved, StateAvailable) }

// Complete: Assigned -> Available (the trip finished or was cancelled).
func (idx *Index) Complete(id string) bool { return idx.transition(id, StateAssigned, StateAvailable) }

// ReserveNearest finds the closest available drivers and reserves the first one
// it wins. Search is only a hint (its snapshot can be stale); the compare-and-swap
// in Reserve is the truth. If every candidate was taken by other requests in the
// meantime, it searches again. It returns false only when a search finds no
// available driver at all, and each failed pass means other requests made
// progress, so it can't loop forever.
func (idx *Index) ReserveNearest(lat, lng float64, opt SearchOptions) (Match, bool) {
	for {
		matches := idx.Search(lat, lng, opt)
		if len(matches) == 0 {
			return Match{}, false
		}
		for _, m := range matches {
			if idx.Reserve(m.Driver.ID) {
				m.Driver.State = StateReserved
				return m, true
			}
		}
	}
}