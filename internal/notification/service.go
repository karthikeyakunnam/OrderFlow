package notification

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ShivamNayak-dev/cartnova/internal/events"
	"go.mongodb.org/mongo-driver/mongo"
)

type OrderEventPayload struct {
	OrderID int64  `json:"order_id"`
	UserID  int64  `json:"user_id"`
	Reason  string `json:"reason,omitempty"`
}

type Service struct {
	repository *Repository
	hub        *Hub
}

func NewService(repository *Repository, hub *Hub) *Service {
	return &Service{repository: repository, hub: hub}
}

func (s *Service) ConsumeOrderEvents(ctx context.Context, consumer *events.Consumer) error {
	for {
		message, err := consumer.Fetch(ctx)
		if err != nil {
			return err
		}

		if err := s.processEvent(ctx, message.Event); err != nil {
			return err
		}
		if err := consumer.Commit(ctx, message); err != nil {
			return err
		}
	}
}

func (s *Service) processEvent(ctx context.Context, event events.Event) error {
	var payload OrderEventPayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return err
	}

	message := notificationMessage(event.Type, payload.OrderID, payload.Reason)
	if message == "" {
		return nil
	}

	created, err := s.repository.Create(ctx, event.ID, payload.UserID, payload.OrderID, event.Type, message)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			_, markErr := s.repository.MarkProcessed(ctx, event.ID)
			return markErr
		}
		return err
	}

	s.hub.Broadcast(payload.UserID, created)
	_, err = s.repository.MarkProcessed(ctx, event.ID)
	return err
}

func notificationMessage(eventType string, orderID int64, reason string) string {
	switch eventType {
	case "OrderCreated":
		return fmt.Sprintf("Order #%d has been created.", orderID)
	case "OrderConfirmed":
		return fmt.Sprintf("Order #%d has been confirmed.", orderID)
	case "OrderCancelled":
		if reason != "" {
			return fmt.Sprintf("Order #%d was cancelled: %s", orderID, reason)
		}
		return fmt.Sprintf("Order #%d was cancelled.", orderID)
	case "OrderProcessing":
		return fmt.Sprintf("Order #%d is being processed.", orderID)
	case "OrderShipped":
		return fmt.Sprintf("Order #%d has been shipped.", orderID)
	case "OrderDelivered":
		return fmt.Sprintf("Order #%d has been delivered.", orderID)
	default:
		return ""
	}
}
