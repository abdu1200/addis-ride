package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"ride-sharing/shared/env"
	"ride-sharing/shared/messaging"
	"ride-sharing/shared/tracing"
	"syscall"

	grpcserver "google.golang.org/grpc"
)

var GrpcAddr = ":9092"

func main() {
	// Initialize Tracing
	tracerCfg := tracing.Config{
		ServiceName:    "driver-service",
		Environment:    env.GetString("ENVIRONMENT", "development"),
		JaegerEndpoint: env.GetString("JAEGER_ENDPOINT", "http://jaeger:14268/api/traces"),
	}

	sh, err := tracing.InitTracer(tracerCfg)
	if err != nil {
		log.Fatalf("Failed to initialize the tracer: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer sh(ctx)

	rabbitMqURI := env.GetString("RABBITMQ_URI", "amqp://guest:guest@rabbitmq:5672/")

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
		<-sigCh
		cancel()
	}()

	lis, err := net.Listen("tcp", GrpcAddr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	svc := NewService()

	// RabbitMQ connection
	rabbitmq, err := messaging.NewRabbitMQ(rabbitMqURI)
	if err != nil {
		log.Fatal(err)
	}
	defer rabbitmq.Close()

	log.Println("Starting RabbitMQ connection")

	offers := offer.New(svc.mat, svc.idx, &rabbitNotifier{rabbitmq: rabbitmq}, offer.Config{
		OfferTimeout: 15 * time.Second,
		MatchTimeout: 3 * time.Second,
		MaxAttempts:  5,
	}) //manager
	defer offers.Close()

	// Starting the gRPC server
	grpcServer := grpcserver.NewServer(tracing.WithTracingInterceptors()...)
	NewGrpcHandler(grpcServer, svc)

	tripconsumer := NewTripConsumer(rabbitmq, svc, offers)
	go func() {
		if err := tripconsumer.Listen(); err != nil {
			log.Fatalf("Failed to listen to the trip message: %v", err)
		}
	}()

	locConsumer := NewLocationConsumer(rabbitmq, svc) // new
	go func() {
		if err := locConsumer.Listen(); err != nil {
			log.Fatalf("Failed to listen to the location message: %v", err)
		}
	}()

	drivConsumer := NewDriverResponseConsumer(rabbitmq, svc, offers)
	go func() {
		if err := drivConsumer.Listen(); err != nil {
			log.Fatalf("Failed to listen to the driver response message: %v", err)
		}
	}()

	paymConsumer := NewPaymentConsumer(rabbitmq, svc)   
	go func() {
		if err := paymConsumer.Listen(); err != nil {
			log.Fatalf("Failed to listen to the payment success message: %v", err)
		}
	}()

	log.Printf("Starting gRPC server Driver service on port %s", lis.Addr().String())

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Printf("failed to serve: %v", err)
			cancel()
		}
	}()

	// wait for the shutdown signal
	<-ctx.Done()
	log.Println("Shutting down the server...")
	grpcServer.GracefulStop()
}
