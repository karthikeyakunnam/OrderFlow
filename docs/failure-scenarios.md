# Failure Scenarios to Study

CartNova is intentionally suitable for failure-oriented reverse engineering.

## Kafka unavailable

Order creation commits an `OrderCreated` record to the Order Service outbox in the same PostgreSQL transaction as the order. If Kafka is unavailable, the outbox publisher retries delivery without losing the order event.

## Redis unavailable

Product Service should still be able to read from PostgreSQL. Cache failures are treated as cache misses.

## Inventory database unavailable

The inventory consumer cannot complete the reservation and should not acknowledge the Kafka message until the business operation is successfully handled.

## Duplicate event

Consumers use event IDs and deterministic derived IDs to avoid applying the same business event repeatedly. Delivery is at-least-once, so idempotency is part of the business workflow rather than a Kafka-only setting.

## Client disconnects

The WebSocket connection is removed from the notification hub.

## Request cancellation

`context.Context` propagates cancellation to downstream work and background processing.

## Concurrent stock requests

PostgreSQL row locking prevents two transactions from both consuming the same available stock.
