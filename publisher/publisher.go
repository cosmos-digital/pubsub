package publisher

import (
	"context"
	"fmt"

	pubsub "github.com/cosmos-digital/pubsub/internal"
)

func New(ctx context.Context, projectID string, topic string) (*pubsub.Publisher, error) {
	connection := pubsub.New(projectID)
	instance, err := connection.Connect(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create pubsub client: %w", err)
	}
	return instance.Publisher(topic), nil
}
