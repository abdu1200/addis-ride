package main

import (
	"context"
	"encoding/json"
	"log"
	"ride-sharing/shared/contracts"
	"ride-sharing/shared/messaging"

	"github.com/rabbitmq/amqp091-go"
)

type driverResponseConsumer struct {
	rabbitmq *messaging.RabbitMQ
	service  *Service
	offers *offer.Manager

}

func NewDriverResponseConsumer(r *messaging.RabbitMQ, s *Service, o *offer.Manager) *driverResponseConsumer {
	return &driverResponseConsumer{rabbitmq: r, service: s, offers: o}
}

func (c *driverResponseConsumer) Listen() error {
	return c.rabbitmq.ConsumeMessages(messaging.DriverServiceTripResponseQueue, func(ctx context.Context, msg amqp091.Delivery) error {
		var message contracts.AmqpMessage
		if err := json.Unmarshal(msg.Body, &message); err != nil {
			log.Printf("Failed to unmarshal message: %v", err)
			return nil // a retry can't fix a malformed message
		}

		driverID := message.OwnerID
		if driverID == "" {
			log.Printf("driver response without a driver ID")
			return nil
		}

		switch msg.RoutingKey {
		case contracts.DriverCmdTripAccept:
			if !c.offers.OnAccept(payload.TripID, driverID) {
				log.Printf("accept for unknown or finished trip %s", payload.TripID)
			}
		case contracts.DriverCmdTripDecline:
			c.offers.OnDecline(payload.TripID, driverID)
		default:
			log.Printf("unknown driver response key: %s", msg.RoutingKey)
		}

		return nil
	})
}