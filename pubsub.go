package pubsub

import (
	"context"
	"fmt"

	gpubsub "cloud.google.com/go/pubsub"

	pubsub "github.com/cosmos-digital/pubsub/internal"
	"github.com/cosmos-digital/pubsub/publisher"
	"github.com/cosmos-digital/pubsub/subscriber"
)

type Message = pubsub.Message

type ErrTopicAlreadyExists = pubsub.ErrTopicAlreadyExists
type ErrTopicNotFound = pubsub.ErrTopicNotFound

type ErrSubscriptionAlreadyExists = pubsub.ErrSubscriptionAlreadyExists
type ErrSubscriptionNotFound = pubsub.ErrSubscriptionNotFound

func New(ctx context.Context, projectID string) (*pubsub.Instance, error) {
	client, err := gpubsub.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %w", err)
	}
	return pubsub.New(client), nil
}

func NewSubscriber(ctx context.Context, projectID string) (*subscriber.Client, error) {
	instance, err := New(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("failed to create instance: %w", err)
	}
	subscriber, err := subscriber.New(ctx, instance)
	if err != nil {
		return nil, fmt.Errorf("failed to create subscriber: %w", err)
	}
	return subscriber, nil
}

func NewPublisher(ctx context.Context, projectID string) (*publisher.Client, error) {
	instance, err := New(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("failed to create instance: %w", err)
	}
	publisher, err := publisher.New(ctx, instance)
	if err != nil {
		return nil, fmt.Errorf("failed to create publisher: %w", err)
	}
	return publisher, nil
}
