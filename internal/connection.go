package pubsub

import (
	"context"
	"fmt"

	"cloud.google.com/go/pubsub"
)

type Connection struct {
	projectID string
}

func New(projectID string) *Connection {
	return &Connection{
		projectID: projectID,
	}
}

func (c *Connection) Connect(ctx context.Context) (*Instance, error) {
	client, err := pubsub.NewClient(ctx, c.projectID)
	if err != nil {
		return nil, fmt.Errorf("failed to create pubsub client: %w", err)
	}
	return &Instance{
		client: client,
	}, nil
}
