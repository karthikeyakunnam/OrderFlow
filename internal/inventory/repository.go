package inventory

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/karthikeyakunnam/OrderFlow/internal/events"
	"github.com/karthikeyakunnam/OrderFlow/internal/outbox"
)

var ErrNotFound = errors.New("inventory not found")
var ErrInsufficientStock = errors.New("insufficient stock")

type Repository struct {
	db *pgxpool.Pool
}

func NewRepository(db *pgxpool.Pool) *Repository {
	return &Repository{db: db}
}

func (r *Repository) CreateOrUpdate(ctx context.Context, productID int64, quantity int) (*Inventory, error) {
	inventory := &Inventory{}
	err := r.db.QueryRow(ctx, `
		INSERT INTO inventory(product_id, available_quantity)
		VALUES($1, $2)
		ON CONFLICT(product_id)
		DO UPDATE SET available_quantity = inventory.available_quantity + EXCLUDED.available_quantity,
		              updated_at = NOW()
		RETURNING product_id, available_quantity, reserved_quantity
	`, productID, quantity).Scan(&inventory.ProductID, &inventory.AvailableQuantity, &inventory.ReservedQuantity)
	return inventory, err
}

func (r *Repository) ReserveOrder(ctx context.Context, orderID int64, items []ReservationItem) error {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if err := reserveOrderTx(ctx, tx, orderID, items); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (r *Repository) ReserveOrderWithOutbox(ctx context.Context, orderID int64, items []ReservationItem, event events.Event) error {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if err := reserveOrderTx(ctx, tx, orderID, items); err != nil {
		return err
	}
	if err := outbox.EnqueueTx(ctx, tx, InventoryEventsTopic, event); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func reserveOrderTx(ctx context.Context, tx pgx.Tx, orderID int64, items []ReservationItem) error {
	for _, item := range items {
		var alreadyReserved bool
		err := tx.QueryRow(ctx, `
			SELECT EXISTS(
				SELECT 1
				FROM reservations
				WHERE order_id = $1
				  AND product_id = $2
				  AND status = 'RESERVED'
			)
		`, orderID, item.ProductID).Scan(&alreadyReserved)
		if err != nil {
			return err
		}
		if alreadyReserved {
			continue
		}

		var available int
		err = tx.QueryRow(ctx, `
			SELECT available_quantity
			FROM inventory
			WHERE product_id = $1
			FOR UPDATE
		`, item.ProductID).Scan(&available)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if available < item.Quantity {
			return ErrInsufficientStock
		}

		_, err = tx.Exec(ctx, `
			UPDATE inventory
			SET available_quantity = available_quantity - $1,
				reserved_quantity = reserved_quantity + $1,
				updated_at = NOW()
			WHERE product_id = $2
		`, item.Quantity, item.ProductID)
		if err != nil {
			return err
		}

		_, err = tx.Exec(ctx, `
			INSERT INTO reservations(order_id, product_id, quantity, status)
			VALUES($1, $2, $3, 'RESERVED')
		`, orderID, item.ProductID, item.Quantity)
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *Repository) EnqueueEvent(ctx context.Context, event events.Event) error {
	return outbox.Enqueue(ctx, r.db, InventoryEventsTopic, event)
}

func (r *Repository) FinalizeOrder(ctx context.Context, eventID string, orderID int64, release bool) (bool, error) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)

	var inserted bool
	if err := tx.QueryRow(ctx, `
		INSERT INTO processed_events(event_id)
		VALUES($1)
		ON CONFLICT DO NOTHING
		RETURNING TRUE
	`, eventID).Scan(&inserted); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, tx.Commit(ctx)
		}
		return false, err
	}

	rows, err := tx.Query(ctx, `
		SELECT product_id, quantity
		FROM reservations
		WHERE order_id = $1 AND status = 'RESERVED'
		FOR UPDATE
	`, orderID)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	type reservation struct {
		productID int64
		quantity  int
	}
	reservations := make([]reservation, 0)
	for rows.Next() {
		var item reservation
		if err := rows.Scan(&item.productID, &item.quantity); err != nil {
			return false, err
		}
		reservations = append(reservations, item)
	}
	if err := rows.Err(); err != nil {
		return false, err
	}

	for _, item := range reservations {
		if release {
			_, err = tx.Exec(ctx, `
				UPDATE inventory
				SET available_quantity = available_quantity + $1,
				    reserved_quantity = reserved_quantity - $1,
				    updated_at = NOW()
				WHERE product_id = $2
			`, item.quantity, item.productID)
		} else {
			_, err = tx.Exec(ctx, `
				UPDATE inventory
				SET reserved_quantity = reserved_quantity - $1,
				    updated_at = NOW()
				WHERE product_id = $2
			`, item.quantity, item.productID)
		}
		if err != nil {
			return false, err
		}
	}

	status := "COMMITTED"
	if release {
		status = "RELEASED"
	}
	if _, err := tx.Exec(ctx, `
		UPDATE reservations
		SET status = $1
		WHERE order_id = $2 AND status = 'RESERVED'
	`, status, orderID); err != nil {
		return false, err
	}

	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return inserted, nil
}

func (r *Repository) Get(ctx context.Context, productID int64) (*Inventory, error) {
	inventory := &Inventory{}
	err := r.db.QueryRow(ctx, `
		SELECT product_id, available_quantity, reserved_quantity
		FROM inventory
		WHERE product_id = $1
	`, productID).Scan(&inventory.ProductID, &inventory.AvailableQuantity, &inventory.ReservedQuantity)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return inventory, err
}

func (r *Repository) MarkEventProcessed(ctx context.Context, eventID string) (bool, error) {
	result, err := r.db.Exec(ctx, `
		INSERT INTO processed_events(event_id)
		VALUES($1)
		ON CONFLICT DO NOTHING
	`, eventID)
	if err != nil {
		return false, err
	}
	return result.RowsAffected() == 1, nil
}
