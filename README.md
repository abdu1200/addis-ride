# Addis Ride

Addis Ride is an event-driven microservices ride-hailing platform designed for real-time driver matching, trip routing, and asynchronous payment settlement.

---

## System Architecture

The architecture uses an API Gateway serving client applications over REST and WebSockets, internal microservices communicating via gRPC, and an asynchronous messaging backbone powered by RabbitMQ topic exchanges.

<div class="diagram-wrap">
  <svg viewBox="0 0 780 400" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Addis Ride architecture diagram">
    <defs>
      <marker id="arrow" markerWidth="8" markerHeight="8" refX="6" refY="3" orient="auto">
        <path d="M0,0 L6,3 L0,6 Z" fill="#2A3240"/>
      </marker>
    </defs>
    <!-- clients -->
    <rect x="20" y="20" width="120" height="40" rx="4" fill="none" stroke="#2A3240"/>
    <text x="80" y="44" fill="#8892A0" font-family="IBM Plex Mono" font-size="11.5" text-anchor="middle">rider app</text>
    <rect x="20" y="330" width="120" height="40" rx="4" fill="none" stroke="#2A3240"/>
    <text x="80" y="354" fill="#8892A0" font-family="IBM Plex Mono" font-size="11.5" text-anchor="middle">driver app</text>

    <!-- gateway -->
    <rect x="190" y="170" width="140" height="56" rx="4" fill="#1D232D" stroke="#E8A33D"/>
    <text x="260" y="194" fill="#E7ECF2" font-family="IBM Plex Mono" font-size="12" text-anchor="middle">api-gateway</text>
    <text x="260" y="210" fill="#8892A0" font-family="IBM Plex Mono" font-size="10" text-anchor="middle">REST + WS</text>

    <!-- services -->
    <rect x="400" y="40" width="140" height="52" rx="4" fill="#1D232D" stroke="#2A3240"/>
    <text x="470" y="62" fill="#E7ECF2" font-family="IBM Plex Mono" font-size="12" text-anchor="middle">trip-service</text>
    <text x="470" y="77" fill="#8892A0" font-family="IBM Plex Mono" font-size="10" text-anchor="middle">OSRM · fare · status</text>

    <rect x="400" y="112" width="140" height="52" rx="4" fill="#1D232D" stroke="#2A3240"/>
    <text x="470" y="134" fill="#E7ECF2" font-family="IBM Plex Mono" font-size="12" text-anchor="middle">driver-service</text>
    <text x="470" y="149" fill="#8892A0" font-family="IBM Plex Mono" font-size="10" text-anchor="middle">geohash matching</text>

    <rect x="400" y="184" width="140" height="52" rx="4" fill="#1D232D" stroke="#2A3240"/>
    <text x="470" y="206" fill="#E7ECF2" font-family="IBM Plex Mono" font-size="12" text-anchor="middle">payment-service</text>
    <text x="470" y="221" fill="#8892A0" font-family="IBM Plex Mono" font-size="10" text-anchor="middle">Stripe checkout</text>

    <rect x="400" y="256" width="140" height="52" rx="4" fill="#1D232D" stroke="#2A3240"/>
    <text x="470" y="278" fill="#E7ECF2" font-family="IBM Plex Mono" font-size="12" text-anchor="middle">redis</text>
    <text x="470" y="293" fill="#8892A0" font-family="IBM Plex Mono" font-size="10" text-anchor="middle">WS connection registry</text>

    <!-- rabbitmq -->
    <rect x="600" y="112" width="150" height="80" rx="4" fill="#1D232D" stroke="#4FD1C5"/>
    <text x="675" y="140" fill="#E7ECF2" font-family="IBM Plex Mono" font-size="12" text-anchor="middle">rabbitmq</text>
    <text x="675" y="156" fill="#8892A0" font-family="IBM Plex Mono" font-size="10" text-anchor="middle">topic exchange</text>
    <text x="675" y="171" fill="#8892A0" font-family="IBM Plex Mono" font-size="10" text-anchor="middle">event · cmd</text>

    <!-- observability -->
    <rect x="600" y="256" width="150" height="52" rx="4" fill="#1D232D" stroke="#2A3240"/>
    <text x="675" y="278" fill="#E7ECF2" font-family="IBM Plex Mono" font-size="12" text-anchor="middle">jaeger · prometheus</text>
    <text x="675" y="293" fill="#8892A0" font-family="IBM Plex Mono" font-size="10" text-anchor="middle">tracing + metrics</text>

    <!-- connections -->
    <line x1="140" y1="40" x2="190" y2="190" stroke="#2A3240" marker-end="url(#arrow)"/>
    <line x1="140" y1="350" x2="190" y2="205" stroke="#2A3240" marker-end="url(#arrow)"/>
    <line x1="330" y1="190" x2="400" y2="70" stroke="#2A3240" marker-end="url(#arrow)"/>
    <line x1="330" y1="198" x2="400" y2="140" stroke="#2A3240" marker-end="url(#arrow)"/>
    <line x1="330" y1="206" x2="400" y2="210" stroke="#2A3240" marker-end="url(#arrow)"/>
    <line x1="330" y1="214" x2="400" y2="280" stroke="#2A3240" marker-end="url(#arrow)"/>
    <line x1="540" y1="130" x2="600" y2="152" stroke="#4FD1C5" marker-end="url(#arrow)"/>
    <line x1="540" y1="200" x2="600" y2="160" stroke="#4FD1C5" marker-end="url(#arrow)"/>
    <line x1="540" y1="66" x2="600" y2="140" stroke="#4FD1C5" marker-end="url(#arrow)"/>

    <text x="20" y="392" fill="#2A3240" font-family="IBM Plex Mono" font-size="10">gRPC internal · amber = gateway boundary · teal = async pub/sub</text>
  </svg>
</div>

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