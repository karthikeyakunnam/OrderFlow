package notification

import (
	"context"
	"time"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Repository struct {
	collection *mongo.Collection
	processed  *mongo.Collection
}

func NewRepository(database *mongo.Database) *Repository {
	return &Repository{
		collection: database.Collection("notifications"),
		processed:  database.Collection("processed_events"),
	}
}

func (r *Repository) EnsureIndexes(ctx context.Context) error {
	_, err := r.collection.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "event_id", Value: 1}},
		Options: options.Index().SetUnique(true).SetPartialFilterExpression(bson.M{"event_id": bson.M{"$exists": true}}),
	})
	return err
}

func (r *Repository) MarkProcessed(ctx context.Context, eventID string) (bool, error) {
	_, err := r.processed.InsertOne(ctx, bson.M{"_id": eventID, "processed_at": time.Now().UTC()})
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (r *Repository) Create(ctx context.Context, eventID string, userID int64, orderID int64, eventType string, message string) (*Notification, error) {
	notification := &Notification{
		ID:        uuid.NewString(),
		EventID:   eventID,
		UserID:    userID,
		OrderID:   orderID,
		Type:      eventType,
		Message:   message,
		CreatedAt: time.Now().UTC(),
	}

	_, err := r.collection.InsertOne(ctx, notification)
	return notification, err
}

func (r *Repository) FindByUserID(ctx context.Context, userID int64) ([]Notification, error) {
	cursor, err := r.collection.Find(ctx, bson.M{"user_id": userID})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var notifications []Notification
	if err := cursor.All(ctx, &notifications); err != nil {
		return nil, err
	}
	return notifications, nil
}
