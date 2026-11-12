package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/karthikeyakunnam/OrderFlow/internal/events"
)

type pendingEvent struct {
	id       int64
	topic    string
	eventID  string
	payload  []byte
	attempts int
}

// EnqueueTx stores an already serialized event in the same transaction as the
// business change that produced it.
func EnqueueTx(ctx context.Context, tx pgx.Tx, topic string, event events.Event) error {
	data, err := jsonMarshal(event)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO outbox_events(topic, event_id, payload)
		VALUES($1, $2, $3)
		ON CONFLICT(event_id) DO NOTHING
	`, topic, event.ID, data)
	return err
}

func Enqueue(ctx context.Context, db *pgxpool.Pool, topic string, event events.Event) error {
	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if err := EnqueueTx(ctx, tx, topic, event); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

type Publisher struct {
	db  *pgxpool.Pool
	bus *events.Bus
}

func NewPublisher(db *pgxpool.Pool, bus *events.Bus) *Publisher {
	return &Publisher{db: db, bus: bus}
}

func (p *Publisher) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		p.flush(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (p *Publisher) flush(ctx context.Context) {
	items, err := p.claim(ctx, 50)
	if err != nil {
		return
	}

	for _, item := range items {
		if err := p.bus.PublishRaw(ctx, item.topic, item.eventID, item.payload); err != nil {
			_ = p.fail(ctx, item.id, item.attempts, err)
			continue
		}
		_ = p.complete(ctx, item.id)
	}
}

func (p *Publisher) claim(ctx context.Context, limit int) ([]pendingEvent, error) {
	tx, err := p.db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		SELECT id, topic, event_id, payload, attempts
		FROM outbox_events
		WHERE published_at IS NULL
		  AND available_at <= NOW()
		  AND (locked_until IS NULL OR locked_until < NOW())
		ORDER BY id
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]pendingEvent, 0, limit)
	for rows.Next() {
		var item pendingEvent
		if err := rows.Scan(&item.id, &item.topic, &item.eventID, &item.payload, &item.attempts); err != nil {
			return nil, err
		}
		item.attempts++
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	for _, item := range items {
		if _, err := tx.Exec(ctx, `
			UPDATE outbox_events
			SET attempts = $1, locked_until = NOW() + INTERVAL '30 seconds'
			WHERE id = $2
		`, item.attempts, item.id); err != nil {
			return nil, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return items, nil
}

func (p *Publisher) complete(ctx context.Context, id int64) error {
	_, err := p.db.Exec(ctx, `
		UPDATE outbox_events
		SET published_at = NOW(), locked_until = NULL, last_error = NULL
		WHERE id = $1
	`, id)
	return err
}

func (p *Publisher) fail(ctx context.Context, id int64, attempts int, publishErr error) error {
	delay := time.Duration(1<<min(attempts, 6)) * time.Second
	message := publishErr.Error()
	if len(message) > 1000 {
		message = message[:1000]
	}
	_, err := p.db.Exec(ctx, `
		UPDATE outbox_events
		SET available_at = NOW() + $1::interval,
		    locked_until = NULL,
		    last_error = $2
		WHERE id = $3
	`, fmt.Sprintf("%d seconds", int(delay/time.Second)), message, id)
	return err
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Kept local to avoid coupling callers to JSON details.
func jsonMarshal(value any) ([]byte, error) {
	return json.Marshal(value)
}
