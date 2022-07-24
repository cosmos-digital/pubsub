package publisher

import (
	"context"
	"fmt"

	pubsub "github.com/cosmos-digital/pubsub/internal"
)

type Client struct {
	instance *pubsub.Instance
}

func New(ctx context.Context, instance *pubsub.Instance) (*Client, error) {
	return &Client{
		instance: instance,
	}, nil
}

func (p *Client) Publish(ctx context.Context, topic *pubsub.Topic, message []byte) error {
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

func (p *Client) Close() error {
	if err := p.instance.Close(); err != nil {
		return fmt.Errorf("failed to close client: %w", err)
	}
	return nil
}
