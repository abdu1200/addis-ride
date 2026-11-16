package main

import (
	"context"
	"ride-sharing/shared/contracts"
	"ride-sharing/shared/messaging"

	"github.com/rabbitmq/amqp091-go"
)

type rabbitNotifier struct{ 
	rabbitmq *messaging.RabbitMQ 
}

func (n *rabbitNotifier) SendOffer(ctx context.Context, driverID string, payload []byte) error {
	return n.rabbitmq.PublishMessage(ctx, contracts.DriverCmdTripRequest,
		contracts.AmqpMessage{
			OwnerID: driverID, 
			Data: payload
		}) // Notify the driver about a potential trip
}

func (n *rabbitNotifier) NoDriversFound(ctx context.Context, riderID string) error {
	return n.rabbitmq.PublishMessage(ctx, contracts.TripEventNoDriversFound,
		contracts.AmqpMessage{
			OwnerID: riderID
		})
}