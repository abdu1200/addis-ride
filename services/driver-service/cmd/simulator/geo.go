package main

import (
	"math"
	"math/rand/v2"

	"ride-sharing/services/driver-service/internal/index"
)

const metersPerDeg = 111320.0

type point struct{ lat, lng float64 }

type region struct{ minLat, maxLat, minLng, maxLng float64 }

// A 7.8 km x 7.7 km square around central Addis Ababa.
var city = region{minLat: 8.975, maxLat: 9.045, minLng: 38.715, maxLng: 38.785}

// Busy areas, as fractions of the region: {lng fraction, lat fraction}.
var hotspots = [][2]float64{{0.35, 0.55}, {0.68, 0.35}, {0.50, 0.78}}

func (r region) clamp(p point) point {
	return point{
		lat: math.Min(math.Max(p.lat, r.minLat), r.maxLat),
		lng: math.Min(math.Max(p.lng, r.minLng), r.maxLng),
	}
}

func (r region) uniform() point {
	return point{
		lat: r.minLat + rand.Float64()*(r.maxLat-r.minLat),
		lng: r.minLng + rand.Float64()*(r.maxLng-r.minLng),
	}
}

// hotspot returns a random point clustered around one of the busy areas.
func (r region) hotspot() point {
	h := hotspots[rand.IntN(len(hotspots))]
	lng := r.minLng + (h[0]+rand.NormFloat64()*0.07)*(r.maxLng-r.minLng)
	lat := r.minLat + (h[1]+rand.NormFloat64()*0.07)*(r.maxLat-r.minLat)
	return r.clamp(point{lat, lng})
}

// dropoffNear returns a point 0.8 to 2.5 km from p.
func (r region) dropoffNear(p point) point {
	ang := rand.Float64() * 2 * math.Pi
	dist := 800 + rand.Float64()*1700
	return r.clamp(point{
		lat: p.lat + dist*math.Cos(ang)/metersPerDeg,
		lng: p.lng + dist*math.Sin(ang)/(metersPerDeg*math.Cos(p.lat*math.Pi/180)),
	})
}

// wander moves a point along a heading and turns around at the edge.
func (r region) wander(p point, heading, meters float64) (point, float64) {
	np := point{
		lat: p.lat + meters*math.Cos(heading)/metersPerDeg,
		lng: p.lng + meters*math.Sin(heading)/(metersPerDeg*math.Cos(p.lat*math.Pi/180)),
	}
	if np.lat < r.minLat || np.lat > r.maxLat || np.lng < r.minLng || np.lng > r.maxLng {
		return p, heading + math.Pi
	}
	return np, heading
}

// norm maps a point to 0..10000 on both axes (y = 0 is north), for the browser.
func (r region) norm(p point) (int, int) {
	x := (p.lng - r.minLng) / (r.maxLng - r.minLng)
	y := 1 - (p.lat-r.minLat)/(r.maxLat-r.minLat)
	return clampInt(int(math.Round(x*10000)), 0, 10000), clampInt(int(math.Round(y*10000)), 0, 10000)
}

func (r region) widthMeters() float64 {
	mid := (r.minLat + r.maxLat) / 2
	return (r.maxLng - r.minLng) * metersPerDeg * math.Cos(mid*math.Pi/180)
}

func (r region) heightMeters() float64 { return (r.maxLat - r.minLat) * metersPerDeg }

// moveToward moves `from` up to `meters` toward `to` and reports if it arrived.
func moveToward(from, to point, meters float64) (point, bool) {
	d := index.Haversine(from.lat, from.lng, to.lat, to.lng)
	if d <= meters || d < 1 {
		return to, true
	}
	f := meters / d
	return point{from.lat + (to.lat-from.lat)*f, from.lng + (to.lng-from.lng)*f}, false
}

func clampInt(v, lo, hi int) int { return min(max(v, lo), hi) }