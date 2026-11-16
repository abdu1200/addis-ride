package main

import (
	math "math/rand/v2"
	pb "ride-sharing/shared/proto/driver"
	"ride-sharing/shared/util"
	"sync"
	"fmt"

	"github.com/mmcloughlin/geohash"
	"ride-sharing/services/driver-service/internal/index"
	"ride-sharing/services/driver-service/internal/matcher"
)

type driverInMap struct {
	Driver *pb.Driver
}

type Service struct {
	drivers []*driverInMap
	mu      sync.RWMutex
	idx     *index.Index 
	mat     *matcher.Matcher
}

func NewService() *Service {
	return &Service{
		drivers: make([]*driverInMap, 0),
		idx:     index.New(index.DefaultShards, index.DefaultPrecision),
		mat:     matcher.New(idx, matcher.Config{QueueSize: 1024, SearchLimit: 5, MaxRadiusMeters: 5000}),
	}
}

// this function is not used anymore, replaced by the offer manager
// FindAndReserveNearestDriver returns driver IDs, closest first.
func (s *Service) FindAndReserveNearestDriver(ctx context.Context, slug string, lat, lng float64) (string, bool, error) {
	res, err := s.mat.Match(ctx, matcher.Request{
		PackageSlug: slug,
		Lat:         lat,
		Lng:         lng,
	})
	if err != nil {
		return "", false, err // queue full, timed out, or shutting down
	}
	return res.DriverID, res.Matched, nil
}

// not used anymore, replaced by the offer manager
func (s *Service) ReleaseDriver(id string) {
	 s.idx.Release(id) 
}

// not used anymore, replaced by the offer manager
func (s *Service) AssignDriver(id string) bool { 
	return s.idx.Assign(id) 
}

func (s *Service) CompleteTrip(id string) { 
	s.idx.Complete(id) 
}

func (s *Service) RegisterDriver(driverId string, packageSlug string, name string, phoneNumber string, carPlate string, lat, lng float64) (*pb.Driver, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Put the driver in the geo-index first. It rejects invalid coordinates,
	// so a bad location never creates a half-registered driver.
	if err := s.idx.Upsert(driverId, packageSlug, lat, lng); err != nil {
		return nil, fmt.Errorf("register driver %s: %w", driverId, err)
	}

	randomAvatar := util.GetRandomAvatar(math.IntN(10)) // picks 0 to 9

	driver := &pb.Driver{
		Id:             driverId,
		Geohash:        geohash.Encode(lat, lng), // the frontend expects it 
		Location:       &pb.Location{Latitude: lat, Longitude: lng}, // these guys should be latest datas to be sent to the frontend
		Name:           name,
		PhoneNumber:    phoneNumber,
		PackageSlug:    packageSlug,
		ProfilePicture: randomAvatar,
		CarPlate:       carPlate,
	}
	///////////////// the above 'Geohash and Location' datas are send to the frontend so that the frontend can use them to show the driver on the map.
	s.drivers = append(s.drivers, &driverInMap{
		Driver: driver,
	})

	return driver, nil
}

func (s *Service) UnregisterDriver(driverId string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.idx.Remove(driverId) // remove from the geo-index
	
	for i, driver := range s.drivers {
		if driver.Driver.Id == driverId {
			s.drivers = append(s.drivers[:i], s.drivers[i+1:]...)
			return
		}
	}
}

func (s *Service) UpdateDriverLocation(driverID string, lat, lng float64) bool {
	ok, err := s.idx.UpdateLocation(driverID, lat, lng)
	return ok && err == nil
}

func (s *Service) Close() { 
	s.mat.Close() 
}