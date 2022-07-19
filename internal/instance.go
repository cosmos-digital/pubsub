package pubsub

import (
	"context"
	"errors"
	"fmt"

	"cloud.google.com/go/pubsub"
)

type Client interface {
	Topic(id string) *pubsub.Topic
	CreateTopic(ctx context.Context, id string) (*pubsub.Topic, error)
	Subscription(id string) *pubsub.Subscription
	CreateSubscription(ctx context.Context, id string, config pubsub.SubscriptionConfig) (*pubsub.Subscription, error)
	Close() error
}

type Instance struct {
	client *pubsub.Client
}

type Message = *pubsub.Message

type Handler func(ctx context.Context, msg *pubsub.Message) error

func (i *Instance) Close() error {
	if err := i.client.Close(); err != nil {
		return fmt.Errorf("failed to close client: %w", err)
	}
	return nil
}

func (i *Instance) Subscriber() *Subscriber {
	return &Subscriber{
		instance: i,
		handler:  make(map[string]Handler),
		done:     make(chan bool),
		log:      make(chan string),
	}
}

func (i *Instance) GetSubscription(ctx context.Context, name string) (*pubsub.Subscription, error) {
	subscription := i.client.Subscription(name)

	exists, err := subscription.Exists(ctx)
	if err != nil {
		return subscription, fmt.Errorf("failed to check if subscription exists: %w", err)
	}

	if !exists {
		return subscription, ErrSubscriptionNotFound{}
	}

	return subscription, nil
}

func (i *Instance) CreateSubscription(ctx context.Context, name string, topic *pubsub.Topic) (*pubsub.Subscription, error) {
	_, err := i.GetSubscription(ctx, name)
	if err != nil && !errors.Is(err, ErrSubscriptionNotFound{}) {
		return nil, fmt.Errorf("failed to get subscription %s: %w", name, err)
	}
	config := pubsub.SubscriptionConfig{
		Topic: topic,
	}

	subscription, err := i.client.CreateSubscription(ctx, name, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create subscription %w", err)
	}

	return subscription, nil
}

func (i *Instance) Publisher(topicName string) *Publisher {
	return &Publisher{
		instance: i,
		topic:    topicName,
	}
}

func (i *Instance) GetTopic(ctx context.Context, name string) (*pubsub.Topic, error) {
	topic := i.client.Topic(name)

	exists, err := topic.Exists(ctx)
	if err != nil {
		return topic, fmt.Errorf("failed to check if topic exists: %w", err)
	}

	if !exists {
		return topic, ErrTopicNotFound{}
	}

	return topic, nil
}

func (i *Instance) CreateTopic(ctx context.Context, name string) (*pubsub.Topic, error) {
	_, err := i.GetTopic(ctx, name)
	if err != nil && !errors.Is(err, ErrTopicNotFound{}) {
		return nil, fmt.Errorf("failed to get topic %s: %w", name, err)
	}
	topic, err := i.client.CreateTopic(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("failed to create topic %w", err)
	}

	return topic, nil
}
