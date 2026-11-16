// Index is an in-memory, sharded geo-index of cells and drivers.

package index

import (
	"errors"
	"math"
	"sync"
	"time"
	"sync/atomic"

	"github.com/mmcloughlin/geohash"
)

const (
	// DefaultPrecision 6 is roughly 1.2 km x 0.6 km per cell.
	DefaultPrecision uint = 6
	DefaultShards         = 64
)

var ErrInvalidInput = errors.New("index: invalid driver id or coordinates")

// State is the driver's matching state. Feature 4 (reservation) will
// build on this; for now the index only stores it.
type State int32

const (
	StateAvailable State = iota
	StateReserved
	StateAssigned
	StateOffline // StateOffline is set when a driver is removed, so a reservation that was already in flight can't succeed on a driver who just left.
)

func (s State) String() string {
	switch s {
	case StateAvailable:
		return "available"
	case StateReserved:
		return "reserved"
	case StateAssigned:
		return "assigned"
	case StateOffline:
		return "offline"
	}
	return "unknown"
}

// Driver is a snapshot. Values returned by the index are copies,
// so callers can read them without holding any lock.
type Driver struct {
	ID          string
	PackageSlug string
	Lat, Lng    float64
	CellID      string
	UpdatedAt   time.Time
	State       State // copied from st when the snapshot was taken
	st *atomic.Int32 // st is the real state. All copies of this driver share the same pointer.
}

// withState returns a copy whose State field is the current atomic state.
func (d Driver) withState() Driver {
	if d.st != nil {
		d.State = State(d.st.Load())
	}
	return d
}

type driverShard struct {
	mu     sync.RWMutex
	cellOf map[string]string // driverID -> cellID
	states map[string]*atomic.Int32 // driverID -> state
}

type cellShard struct {
	mu    sync.RWMutex
	cells map[string]map[string]Driver // cellID -> driverID -> Driver
}

func (cs *cellShard) put(d Driver) {
	m := cs.cells[d.CellID]
	if m == nil {
		m = make(map[string]Driver)
		cs.cells[d.CellID] = m
	}
	m[d.ID] = d
}

func (cs *cellShard) del(cell, id string) {
	m := cs.cells[cell]
	delete(m, id)
	if len(m) == 0 {
		delete(cs.cells, cell)
	}
}

type Index struct {
	precision uint
	drivers   []driverShard
	cells     []cellShard
}

// New creates an index. shards=1 gives a single-lock baseline for benchmarks.
func New(shards int, precision uint) *Index {
	if shards < 1 {
		shards = 1
	}
	if precision == 0 {
		precision = DefaultPrecision
	}
	idx := &Index{
		precision: precision,
		drivers:   make([]driverShard, shards),
		cells:     make([]cellShard, shards),
	}
	for i := range idx.drivers {
		idx.drivers[i].cellOf = make(map[string]string)
		idx.drivers[i].states = make(map[string]*atomic.Int32)
	}
	for i := range idx.cells {
		idx.cells[i].cells = make(map[string]map[string]Driver)
	}
	return idx
}

// Cell returns the geohash cell ID for a coordinate (used by search later).
func (idx *Index) Cell(lat, lng float64) string {
	return geohash.EncodeWithPrecision(lat, lng, idx.precision)
}

// Upsert adds a driver or moves/updates an existing one.
// An existing driver keeps their State.
func (idx *Index) Upsert(id, packageSlug string, lat, lng float64) error {
	if id == "" || math.IsNaN(lat) || math.IsNaN(lng) ||
		lat < -90 || lat > 90 || lng < -180 || lng > 180 {
		return ErrInvalidInput
	}

	cell := idx.Cell(lat, lng)
	newIdx := idx.cellShardIndex(cell)

	d := Driver{
		ID:          id,
		PackageSlug: packageSlug,
		Lat:         lat,
		Lng:         lng,
		CellID:      cell,
		UpdatedAt:   time.Now(),
	}

	ds := idx.driverShardFor(id)
	ds.mu.Lock()
	defer ds.mu.Unlock()

	// One atomic state per driver, created once and then reused on every move.
	st := ds.states[id]
	if st == nil {
		st = new(atomic.Int32) // zero value is StateAvailable
		ds.states[id] = st
	}
	d.st = st

	oldCell, existed := ds.cellOf[id]
	if !existed {
		cs := &idx.cells[newIdx]
		cs.mu.Lock()
		cs.put(d)
		cs.mu.Unlock()
		ds.cellOf[id] = cell
		return nil
	}

	oldIdx := idx.cellShardIndex(oldCell)
	idx.lockCells(oldIdx, newIdx)
	if oldCell != cell {
		idx.cells[oldIdx].del(oldCell, id)
	}
	idx.cells[newIdx].put(d)
	idx.unlockCells(oldIdx, newIdx)

	ds.cellOf[id] = cell
	return nil
}

// UpdateLocation moves an EXISTING driver to a new location.
// It keeps the driver's package slug and state, and it never adds a driver:
// if the driver isn't in the index, it returns false and does nothing
func (idx *Index) UpdateLocation(id string, lat, lng float64) (bool, error) {
	if id == "" || math.IsNaN(lat) || math.IsNaN(lng) ||
		lat < -90 || lat > 90 || lng < -180 || lng > 180 {
		return false, ErrInvalidInput
	}

	newCell := idx.Cell(lat, lng)
	newIdx := idx.cellShardIndex(newCell)

	ds := idx.driverShardFor(id)
	ds.mu.Lock()
	defer ds.mu.Unlock()

	oldCell, ok := ds.cellOf[id]
	if !ok {
		return false, nil
	}
	oldIdx := idx.cellShardIndex(oldCell)

	idx.lockCells(oldIdx, newIdx)
	d, ok := idx.cells[oldIdx].cells[oldCell][id]
	if !ok {
		idx.unlockCells(oldIdx, newIdx)
		return false, nil
	}
	d.Lat, d.Lng = lat, lng
	d.CellID = newCell
	d.UpdatedAt = time.Now()

	if oldCell != newCell {
		idx.cells[oldIdx].del(oldCell, id)
	}
	idx.cells[newIdx].put(d)
	idx.unlockCells(oldIdx, newIdx)

	ds.cellOf[id] = newCell
	return true, nil
}

// Remove deletes a driver (e.g. on disconnect). Returns false if unknown.
func (idx *Index) Remove(id string) bool {
	ds := idx.driverShardFor(id)
	ds.mu.Lock()
	defer ds.mu.Unlock()

	cell, ok := ds.cellOf[id]
	if !ok {
		return false
	}

	// Mark offline FIRST: a reservation that already holds this pointer will now fail its compare-and-swap instead of "winning" a gone driver.
	if st := ds.states[id]; st != nil {
		st.Store(int32(StateOffline))
	}
	delete(ds.states, id)

	cs := &idx.cells[idx.cellShardIndex(cell)]
	cs.mu.Lock()
	cs.del(cell, id)
	cs.mu.Unlock()

	delete(ds.cellOf, id)
	return true
}

// Get returns a copy of the driver's current data.
func (idx *Index) Get(id string) (Driver, bool) {
	ds := idx.driverShardFor(id)
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	cell, ok := ds.cellOf[id]
	if !ok {
		return Driver{}, false
	}
	cs := &idx.cells[idx.cellShardIndex(cell)]
	cs.mu.RLock()
	d, ok := cs.cells[cell][id]
	cs.mu.RUnlock()
	if !ok {
		return Driver{}, false
	}
	return d.withState(), true
}

// CellDrivers returns copies of all drivers in one cell.
// The read lock is released before the caller does any work with the result.
func (idx *Index) CellDrivers(cell string) []Driver {
	cs := &idx.cells[idx.cellShardIndex(cell)]
	cs.mu.RLock()
	m := cs.cells[cell]
	out := make([]Driver, 0, len(m))
	for _, d := range m {
		out = append(out, d.withState())
	}
	cs.mu.RUnlock()
	return out
}

// Len returns the number of drivers in the index.
func (idx *Index) Len() int {
	n := 0
	for i := range idx.drivers {
		ds := &idx.drivers[i]
		ds.mu.RLock()
		n += len(ds.cellOf)
		ds.mu.RUnlock()
	}
	return n
}

// ---- helpers ----

func (idx *Index) driverShardFor(id string) *driverShard {
	return &idx.drivers[hashString(id)%uint32(len(idx.drivers))]
}

func (idx *Index) cellShardIndex(cell string) int {
	return int(hashString(cell) % uint32(len(idx.cells)))
}

// lockCells locks one or two cell shards in ascending order (deadlock-free).
func (idx *Index) lockCells(a, b int) {
	if a == b {
		idx.cells[a].mu.Lock()
		return
	}
	if a > b {
		a, b = b, a
	}
	idx.cells[a].mu.Lock()
	idx.cells[b].mu.Lock()
}

func (idx *Index) unlockCells(a, b int) {
	idx.cells[a].mu.Unlock()
	if a != b {
		idx.cells[b].mu.Unlock()
	}
}

// hashString is allocation-free FNV-1a.
func hashString(s string) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}