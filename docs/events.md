# OrderFlow Event Catalog

## Kafka topics

### `order-events`

Produced by Order Service and consumed by Inventory and Notification services.

Important event types:

- `OrderCreated`
- `OrderConfirmed`
- `OrderCancelled`
- `OrderProcessing`
- `OrderShipped`
- `OrderDelivered`

### `inventory-events`

Produced by Inventory Service and consumed by Order Service.

Event types:

- `InventoryReserved`
- `InventoryReservationFailed`

## Event envelope

Every event uses the same envelope:

```json
{
  "id": "uuid",
  "type": "OrderCreated",
  "occurred_at": "2026-09-08T00:00:00Z",
  "payload": {}
}
```

The event ID is used as an idempotency key by consumers. Events produced as a consequence of another event use deterministic derived IDs, which makes retries safe.

Order and Inventory Services persist outgoing events in a transactional outbox before a publisher sends them to Kafka. Publishers retry with backoff and keep the event pending until Kafka acknowledges it.

## Order creation flow

```text
Client
  |
  v
API Gateway
  |
  v
Order Service
  |
  | OrderCreated
  v
Kafka: order-events
  |
  +----------------------+
  |                      |
  v                      v
Inventory Service    Notification Service
  |                      |
  | InventoryReserved    +--> MongoDB
  v                      +--> WebSocket
Kafka: inventory-events
  |
  v
Order Service
  |
  v
CONFIRMED
```

If inventory cannot satisfy the request, Inventory publishes `InventoryReservationFailed` and Order changes the order to `CANCELLED`.
