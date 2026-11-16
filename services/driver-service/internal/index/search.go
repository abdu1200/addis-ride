package index

import (
	"cmp"
	"math"
	"slices"

	"github.com/mmcloughlin/geohash"
)

const (
	DefaultMaxRadiusMeters = 5000.0
	metersPerDegree        = 111320.0
	maxRingsCap            = 60
)

// SearchOptions controls a nearest-driver search.
type SearchOptions struct {
	PackageSlug     string  // only drivers with this slug ("" = any)
	Limit           int     // return at most this many drivers (N)
	MaxRadiusMeters float64 // 0 = DefaultMaxRadiusMeters
	Exclude         map[string]struct{} // driver IDs to skip
}

// Match is a driver snapshot plus its distance from the pickup point.
type Match struct {
	Driver         Driver
	DistanceMeters float64
}

// Search returns the nearest available drivers to (lat, lng), closest first.
//
// How it works:
//  1. Find the pickup's cell and scan it (ring 0).
//  2. Scan ring 1 (the 8 cells around it), then ring 2, and so on.
//  3. Stop when we have Limit drivers and the Limit-th one is closer than the
//     distance already fully covered by the rings scanned (k * smallest cell
//     side). Nobody unscanned can be closer than that, so the answer is exact.
//
// Read locks are held only while copying a cell's matching drivers. The
// distance math and sorting happen after the lock is released.
//
// The search is not one atomic snapshot: a driver can move while we scan, so
// we de-duplicate by driver ID. State may also be stale by the time the caller
// uses the result, which is why Feature 4 reserves with compare-and-swap.
func (idx *Index) Search(lat, lng float64, opt SearchOptions) []Match {
	if opt.Limit <= 0 || math.IsNaN(lat) || math.IsNaN(lng) ||
		lat < -90 || lat > 90 || lng < -180 || lng > 180 {
		return nil
	}
	maxRadius := opt.MaxRadiusMeters
	if maxRadius <= 0 {
		maxRadius = DefaultMaxRadiusMeters
	}

	// Geometry of the pickup's cell: center and size.
	box := geohash.BoundingBox(idx.Cell(lat, lng))
	cLat, cLng := box.Center()
	dLat := box.MaxLat - box.MinLat
	dLng := box.MaxLng - box.MinLng
	minDim := math.Min(dLat*metersPerDegree, dLng*metersPerDegree*math.Cos(cLat*math.Pi/180))
	if minDim < 1 {
		minDim = 1
	}
	maxRings := int(maxRadius/minDim) + 1
	if maxRings > maxRingsCap {
		maxRings = maxRingsCap
	}

	visited := make(map[string]struct{}, 32)
	seen := make(map[string]struct{}, opt.Limit*2)
	cands := make([]Driver, 0, 64)
	matches := make([]Match, 0, opt.Limit*2)

	for k := 0; k <= maxRings; k++ {
		// 1. Copy matching drivers out of this ring's cells (short read locks).
		cands = cands[:0]
		for _, cell := range idx.ringCells(cLat, cLng, dLat, dLng, k) {
			if _, ok := visited[cell]; ok {
				continue
			}
			visited[cell] = struct{}{}
			cands = idx.collect(cell, opt.PackageSlug, opt.Exclude, cands)
		}

		// 2. Locks are released. Now do the math.
		if len(cands) > 0 {
			for _, d := range cands {
				if _, dup := seen[d.ID]; dup {
					continue
				}
				seen[d.ID] = struct{}{}
				matches = append(matches, Match{
					Driver:         d,
					DistanceMeters: Haversine(lat, lng, d.Lat, d.Lng),
				})
			}
			slices.SortFunc(matches, func(a, b Match) int {
				return cmp.Compare(a.DistanceMeters, b.DistanceMeters)
			})
		}

		// 3. Can we stop?
		covered := float64(k) * minDim
		if len(matches) >= opt.Limit && matches[opt.Limit-1].DistanceMeters <= covered {
			break
		}
		if covered >= maxRadius {
			break
		}
	}

	out := matches[:0]
	for _, m := range matches {
		if m.DistanceMeters > maxRadius {
			break
		}
		out = append(out, m)
		if len(out) == opt.Limit {
			break
		}
	}
	return out
}

// collect appends the available drivers of one cell (matching slug) to dst.
// Filtering here keeps the copy small. The read lock is held only in here.
func (idx *Index) collect(cell, slug string, exclude map[string]struct{}, dst []Driver) []Driver {
	cs := &idx.cells[idx.cellShardIndex(cell)]
	cs.mu.RLock()
	for _, d := range cs.cells[cell] {
		if _, skip := exclude[d.ID]; skip {
			continue
		}
		s := State(d.st.Load())
		if s != StateAvailable {
			continue
		}
		if slug != "" && d.PackageSlug != slug {
			continue
		}
		d.State = s
		dst = append(dst, d)
	}
	cs.mu.RUnlock()
	return dst
}

// ringCells returns the cell IDs at "distance" k from the center cell:
// k=0 is the cell itself, k=1 the 8 around it, k=2 the 16 around those, etc.
func (idx *Index) ringCells(cLat, cLng, dLat, dLng float64, k int) []string {
	if k == 0 {
		return []string{idx.Cell(cLat, cLng)}
	}
	out := make([]string, 0, 8*k)
	add := func(dy, dx int) {
		la := cLat + float64(dy)*dLat
		if la > 90 || la < -90 {
			return
		}
		out = append(out, idx.Cell(la, wrapLng(cLng+float64(dx)*dLng)))
	}
	for dy := -k; dy <= k; dy++ {
		if dy == -k || dy == k {
			for dx := -k; dx <= k; dx++ {
				add(dy, dx)
			}
		} else {
			add(dy, -k)
			add(dy, k)
		}
	}
	return out
}

func wrapLng(lng float64) float64 {
	for lng >= 180 {
		lng -= 360
	}
	for lng < -180 {
		lng += 360
	}
	return lng
}

// Haversine returns the great-circle distance in meters between two points.
func Haversine(lat1, lng1, lat2, lng2 float64) float64 {
	const earthRadius = 6371000.0
	p1 := lat1 * math.Pi / 180
	p2 := lat2 * math.Pi / 180
	dp := (lat2 - lat1) * math.Pi / 180
	dl := (lng2 - lng1) * math.Pi / 180
	a := math.Sin(dp/2)*math.Sin(dp/2) + math.Cos(p1)*math.Cos(p2)*math.Sin(dl/2)*math.Sin(dl/2)
	return 2 * earthRadius * math.Asin(math.Min(1, math.Sqrt(a)))
}