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

type paymentConsumer struct {
	rabbitmq *messaging.RabbitMQ
	service  *Service
}

func NewPaymentConsumer(r *messaging.RabbitMQ, s *Service) *paymentConsumer {
	return &paymentConsumer{rabbitmq: r, service: s}
}

func (c *paymentConsumer) Listen() error {
	return c.rabbitmq.ConsumeMessages(messaging.DriverServicePaymentSuccessQueue, func(ctx context.Context, msg amqp091.Delivery) error {
		var message contracts.AmqpMessage
		if err := json.Unmarshal(msg.Body, &message); err != nil {
			log.Printf("Failed to unmarshal message: %v", err)
			return nil
		}
		var payload messaging.PaymentStatusUpdateData
		if err := json.Unmarshal(message.Data, &payload); err != nil {
			log.Printf("Failed to unmarshal payload: %v", err)
			return nil
		}

		c.service.CompleteTrip(payload.DriverID)
		return nil
	})
}