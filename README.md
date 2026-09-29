# Addis Ride

Addis Ride is an event-driven microservices ride-sharing platform designed for real-time driver matching, trip routing, and asynchronous payment settlement.

---

## System Architecture

The system uses an API Gateway handling REST and WebSocket client connections, gRPC for internal service communication, and RabbitMQ topic exchanges for event-driven asynchronous messaging.

```mermaid
flowchart LR
    subgraph Clients
        rider["rider app"]
        driver["driver app"]
    end

    subgraph Gateway ["API Gateway Layer"]
        gw["api-gateway<br/>REST + WS"]
    end

    subgraph Services ["Core Services & Storage"]
        trip["trip-service<br/>OSRM · fare · status"]
        driver_svc["driver-service<br/>geohash matching"]
        pay["payment-service<br/>Stripe checkout"]
        redis[("redis<br/>WS connection registry")]
    end

    subgraph Async ["Messaging & Observability"]
        rmq[["rabbitmq<br/>topic exchange (event / cmd)"]]
        obs["jaeger · prometheus<br/>tracing + metrics"]
    end

    rider --> gw
    driver --> gw
    gw --> trip
    gw --> driver_svc
    gw --> pay
    gw --> redis
    trip <--> rmq
    driver_svc <--> rmq
    pay <--> rmq
```
---

## Trip & Payment Lifecycle Flow

The interaction flow below details route calculation via OSRM, driver matching via geohashes, and checkout completion using Stripe webhooks routed through the API Gateway.

```mermaid
sequenceDiagram
    participant User
    participant APIGateway
    participant TripService
    participant DriverService
    participant Driver
    participant PaymentService
    participant StripeAPI
    participant OSRMAPI

    User->>APIGateway: Preview trip route
    APIGateway->>TripService: gRPC: PreviewTrip
    TripService->>OSRMAPI: HTTP: get and calculate route
    OSRMAPI-->>TripService: HTTP: route coordinates
    TripService-->>APIGateway: HTTP: trip route info
    APIGateway-->>User: Display trip preview UI
    User->>APIGateway: Create trip request
    APIGateway->>TripService: gRPC: CreateTrip
    Note over TripService: Creates trip record, status set to requested
    TripService-->>DriverService: trip.event.created (event: trip now exists)
    Note over DriverService: Looks for an available driver to notify
    DriverService-->>APIGateway: driver.cmd.trip_request (command: go offer this trip)
    Note over APIGateway: Receives trip offer command from the broker
    APIGateway->>Driver: WebSocket push: trip request
    Note over Driver: App shows the incoming trip offer
    Driver->>APIGateway: WebSocket: driver.cmd.trip_accept (command: accept this trip)
    Note over APIGateway: Publishes driver's accept command to RabbitMQ
    APIGateway-->>TripService: driver.cmd.trip_accept (command: accept this trip)
    Note over TripService: Updates trip status to accepted and the actual driver info
    TripService-->>APIGateway: trip.event.driver_assigned (event: forward match to rider)
    Note over APIGateway: Receives driver-matched event from the broker
    APIGateway->>User: WebSocket push: driver matched
    Note over User: App shows driver, car, and phone number
    Note over TripService: Requests payment session
    TripService->>PaymentService: payment.event.session_requested (event: trip needs a payment session)
    PaymentService->>StripeAPI: Create checkout session
    StripeAPI-->>PaymentService: Session created
    PaymentService-->>APIGateway: payment.event.session_created (event: session now exists)
    Note over PaymentService: publishes session created event
    APIGateway-->>User: WebSocket push: session info
    Note over User: App shows payment form
    User->>StripeAPI: Complete payment (after meetup and end of drive)
    StripeAPI->>APIGateway: Webhook: payment success
    APIGateway-->>TripService: payment.event.success (event: payment now succeeded)
    Note over TripService: Marks the trip as completed and payed
``` 