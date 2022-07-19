package pubsub

import (
	"context"
	"fmt"

	"cloud.google.com/go/pubsub"
)

type Publisher struct {
	instance *Instance
	topic    string
}

func (p *Publisher) Publish(ctx context.Context, message []byte) error {
	topic, err := p.instance.GetTopic(ctx, p.topic)
	if err != nil {
		return fmt.Errorf("failed to get topic: %w", err)
	}
	result := topic.Publish(ctx, &pubsub.Message{
		Data: message,
	})
	id, err := result.Get(ctx)
	if err != nil {
		return fmt.Errorf("pubsub.Publish: %v", err)
	}
	fmt.Printf("Published a message with ID: %v\n", id)
	return nil
}

func (p *Publisher) Close() error {
	if err := p.instance.Close(); err != nil {
		return fmt.Errorf("failed to close client: %w", err)
	}
	return nil
}
