package order

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/karthikeyakunnam/OrderFlow/internal/events"
	"github.com/karthikeyakunnam/OrderFlow/internal/grpcapi"
	"google.golang.org/protobuf/types/known/structpb"
)

var ErrEmptyItems = errors.New("order must contain at least one item")
var ErrInvalidQuantity = errors.New("quantity must be greater than zero")
var ErrInactiveProduct = errors.New("product is not active")

const OrderEventsTopic = "order-events"
const InventoryEventsTopic = "inventory-events"

type Service struct {
	repository *Repository
	product    *grpcapi.ProductServiceClient
	user       *grpcapi.UserServiceClient
}

func NewService(repository *Repository, productClient *grpcapi.ProductServiceClient, userClient *grpcapi.UserServiceClient) *Service {
	return &Service{
		repository: repository,
		product:    productClient,
		user:       userClient,
	}
}

func (s *Service) Create(ctx context.Context, userID int64, request CreateOrderRequest) (*Order, error) {
	if len(request.Items) == 0 {
		return nil, ErrEmptyItems
	}

	if s.user != nil {
		validationCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		_, err := s.user.GetUser(validationCtx, structValue(map[string]any{"id": float64(userID)}))
		cancel()
		if err != nil {
			return nil, fmt.Errorf("validate user: %w", err)
		}
	}

	quantities := make(map[int64]int, len(request.Items))
	productIDs := make([]int64, 0, len(request.Items))
	for _, requestedItem := range request.Items {
		if requestedItem.ProductID <= 0 || requestedItem.Quantity <= 0 {
			return nil, ErrInvalidQuantity
		}
		if _, exists := quantities[requestedItem.ProductID]; !exists {
			productIDs = append(productIDs, requestedItem.ProductID)
		}
		quantities[requestedItem.ProductID] += requestedItem.Quantity
	}

	items := make([]OrderItem, 0, len(productIDs))
	var total float64

	for _, productID := range productIDs {
		validationCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		response, err := s.product.GetProduct(validationCtx, structValue(map[string]any{
			"id": float64(productID),
		}))
		cancel()
		if err != nil {
			return nil, fmt.Errorf("get product %d: %w", productID, err)
		}
		if response.Fields["status"].GetStringValue() != "ACTIVE" {
			return nil, fmt.Errorf("product %d: %w", productID, ErrInactiveProduct)
		}

		price := response.Fields["price"].GetNumberValue()
		item := OrderItem{
			ProductID: productID,
			Quantity:  quantities[productID],
			Price:     price,
		}
		items = append(items, item)
		total += price * float64(item.Quantity)
	}

	order, err := s.repository.CreateWithOutbox(ctx, userID, items, total, func(order *Order) (events.Event, error) {
		return events.New("OrderCreated", OrderCreatedPayload{
			OrderID: order.ID,
			UserID:  order.UserID,
			Items:   order.Items,
		})
	})
	if err != nil {
		return nil, err
	}

	return order, nil
}

func (s *Service) GetByID(ctx context.Context, id int64) (*Order, error) {
	return s.repository.FindByID(ctx, id)
}

func (s *Service) GetByUserID(ctx context.Context, userID int64) ([]Order, error) {
	return s.repository.FindByUserID(ctx, userID)
}

func (s *Service) UpdateStatus(ctx context.Context, id int64, status string) (*Order, error) {
	if !isAllowedStatus(status) {
		return nil, ErrInvalidStatus
	}

	order, err := s.repository.FindByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if !canTransition(order.Status, status) {
		return nil, ErrInvalidTransition
	}

	event, err := events.New("Order"+statusTitle(status), InventoryResultPayload{
		OrderID: id,
		UserID:  order.UserID,
	})
	if err != nil {
		return nil, err
	}
	if err := s.repository.UpdateStatusWithOutbox(ctx, id, status, event); err != nil {
		return nil, err
	}

	return s.repository.FindByID(ctx, id)
}

func statusTitle(status string) string {
	switch status {
	case "CREATED":
		return "Created"
	case "CONFIRMED":
		return "Confirmed"
	case "PROCESSING":
		return "Processing"
	case "SHIPPED":
		return "Shipped"
	case "DELIVERED":
		return "Delivered"
	case "CANCELLED":
		return "Cancelled"
	default:
		return status
	}
}

func (s *Service) ConsumeInventoryEvents(ctx context.Context, consumer *events.Consumer) error {
	for {
		message, err := consumer.Fetch(ctx)
		if err != nil {
			return err
		}

		if message.Event.Type != "InventoryReserved" && message.Event.Type != "InventoryReservationFailed" {
			if err := consumer.Commit(ctx, message); err != nil {
				return err
			}
			continue
		}

		var payload InventoryResultPayload
		if err := decodePayload(message.Event.Payload, &payload); err != nil {
			return err
		}

		status := "CONFIRMED"
		if message.Event.Type == "InventoryReservationFailed" {
			status = "CANCELLED"
		}

		statusEventType := "OrderConfirmed"
		if status == "CANCELLED" {
			statusEventType = "OrderCancelled"
		}
		statusEvent, err := events.NewDerived(statusEventType, message.Event.ID, payload)
		if err != nil {
			return err
		}
		processed, err := s.repository.ApplyInventoryResult(ctx, message.Event.ID, payload.OrderID, status, statusEvent)
		if err != nil {
			return err
		}
		if !processed {
			return consumer.Commit(ctx, message)
		}

		if err := consumer.Commit(ctx, message); err != nil {
			return err
		}
	}
}

func structValue(value map[string]any) *structpb.Struct {
	result, _ := structpb.NewStruct(value)
	return result
}

func decodePayload(data []byte, target any) error {
	return jsonUnmarshal(data, target)
}
