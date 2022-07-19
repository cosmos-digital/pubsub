package pubsub

import (
	"context"
	"fmt"

	pubsub "bitbucket.org/cosmos-digital/pubsub/internal"
)

type Message = pubsub.Message

func NewSubscriber(ctx context.Context, projectID string) (*pubsub.Subscriber, error) {
	connection := pubsub.New(projectID)
	instance, err := connection.Connect(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create pubsub client: %w", err)
	}
	return instance.Subscriber(), nil
}

func NewPublisher(ctx context.Context, projectID string, topic string) (*pubsub.Publisher, error) {
	connection := pubsub.New(projectID)
	instance, err := connection.Connect(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create pubsub client: %w", err)
	}
	return instance.Publisher(topic), nil
}
