package subscriber

import (
	"context"
	"fmt"

	pubsub "github.com/cosmos-digital/pubsub/internal"
)

func New(ctx context.Context, projectID string) (*pubsub.Subscriber, error) {
	connection := pubsub.New(projectID)
	instance, err := connection.Connect(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create pubsub client: %w", err)
	}
	return instance.Subscriber(), nil
}
