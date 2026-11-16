package main

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"ride-sharing/shared/contracts"
	"ride-sharing/shared/messaging"

	"github.com/rabbitmq/amqp091-go"
)

type tripConsumer struct {
	rabbitmq *messaging.RabbitMQ
	service  *Service
	offers *offer.Manager
}

func NewTripConsumer(rabbitmq *messaging.RabbitMQ, service *Service, offers *offer.Manager) *tripConsumer {
	return &tripConsumer{
		rabbitmq: rabbitmq,
		service:  service,
		offers:   offers,
	}
}

func (c *tripConsumer) Listen() error {
	return c.rabbitmq.ConsumeMessages(messaging.FindAvailableDriversQueue, func(ctx context.Context, msg amqp091.Delivery) error {
		var tripEvent contracts.AmqpMessage
		if err := json.Unmarshal(msg.Body, &tripEvent); err != nil {
			log.Printf("Failed to unmarshal message: %v", err)
			return err
		}

		var payload messaging.TripEventData
		if err := json.Unmarshal(tripEvent.Data, &payload); err != nil {
			log.Printf("Failed to unmarshal message: %v", err)
			return err
		}

		log.Printf("driver received message: %+v", payload)

		switch msg.RoutingKey {
		case contracts.TripEventCreated:
			return c.handleTripCreated(ctx, payload) //payload.Trip  // handles finding and notifiying drivers and more for a given trip
		case contracts.TripEventDriverNotInterested:
			return nil // the offer manager already handles declines (rejections), so this is not needed here anymore

		log.Printf("unknown trip event: %+v", payload)

		return nil
	})
}

func (c *tripConsumer) handleTripCreated(ctx context.Context, payload messaging.TripEventData) error {
	trip := payload.Trip
    geo := payload.Trip.GetRoute().GetGeometry()
	pickup := geo[0].GetCoordinates()[0] // first point (first coordinate) of the route = pickup

	marshalled, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	c.offers.Start(offer.Trip{
		ID:          trip.GetId(),
		RiderID:     trip.GetUserID(),
		PackageSlug: trip.GetSelectedFare().GetPackageSlug(),
		Lat:         pickup.GetLatitude(),
		Lng:         pickup.GetLongitude(),
		Payload:     marshalled,
	})
	return nil
}
