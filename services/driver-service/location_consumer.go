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

type locationConsumer struct {
	rabbitmq *messaging.RabbitMQ
	service  *Service
}

func NewLocationConsumer(r *messaging.RabbitMQ, s *Service) *locationConsumer {
	return &locationConsumer{rabbitmq: r, service: s}
}

func (c *locationConsumer) Listen() error {
	return c.rabbitmq.ConsumeMessages(messaging.DriverLocationQueue, func(ctx context.Context, msg amqp091.Delivery) error {
		var env contracts.AmqpMessage
		if err := json.Unmarshal(msg.Body, &env); err != nil {
			log.Printf("Failed to unmarshal location message: %v", err)
			return nil 
		}

		// Adjust these fields to match what your frontend sends (see below).
		var loc struct {
			Latitude  float64 `json:"latitude"`
			Longitude float64 `json:"longitude"`
		}
		if err := json.Unmarshal(env.Data, &loc); err != nil {
			return nil
		}
		if loc.Latitude == 0 && loc.Longitude == 0 {
			return nil 
		}

		c.service.UpdateDriverLocation(env.OwnerID, loc.Latitude, loc.Longitude)
		return nil
	})
}