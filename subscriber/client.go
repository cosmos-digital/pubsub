package subscriber

import (
	"context"
	"fmt"
	"sync"

	pubsub "github.com/cosmos-digital/pubsub/internal"
)

type Client struct {
	instance *pubsub.Instance
	handler  map[string]pubsub.Handler
	log      chan string
	done     chan bool
}

func New(ctx context.Context, instance *pubsub.Instance) (*Client, error) {
	return &Client{
		instance: instance,
		handler:  make(map[string]pubsub.Handler),
		log:      make(chan string),
		done:     make(chan bool),
	}, nil
}

func (s *Client) AddHandler(subscription string, handler pubsub.Handler) *Client {
	s.handler[subscription] = handler
	return s
}

func (s *Client) Consume(ctx context.Context, wg *sync.WaitGroup) error {
	for subscriptionName, handler := range s.handler {
		wg.Add(1)
		subscription, err := s.instance.GetSubscription(ctx, subscriptionName)
		if err != nil {
			return fmt.Errorf("failed to get subscription %s: %w", subscription, err)
		}
		go func(ctx context.Context, wg *sync.WaitGroup, subscription *pubsub.Subscription, handler pubsub.Handler) {
			defer wg.Done()
			s.log <- fmt.Sprintf("pull message of subscription %s...", subscription.ID())
			if err := subscription.Receive(ctx, func(ctx context.Context, message *pubsub.Message) {
				if err := handler(ctx, message); err != nil {
					s.log <- fmt.Sprint("pubsub handler err: ", err.Error())
					message.Nack()
				}
				message.Ack()
			}); err != nil {
				s.log <- fmt.Sprint("pubsub receive err: ", err.Error())
				s.done <- true
			}
		}(ctx, wg, subscription, handler)
	}
	return nil
}

func (s *Client) Stop() error {
	if err := s.instance.Close(); err != nil {
		return fmt.Errorf("failed to close client: %w", err)
	}
	s.done <- true
	return nil
}

func (s *Client) Log() chan string {
	return s.log
}

func (s *Client) Close() error {
	err := s.instance.Close()
	if err != nil {
		return fmt.Errorf("failed to close client: %w", err)
	}
	close(s.log)
	close(s.done)
	return nil
}
func (s *Client) Done() <-chan bool {
	return s.done
}
func (s *Client) GetSubscription(ctx context.Context, subscription string) (*pubsub.Subscription, error) {
	sub, err := s.instance.GetSubscription(ctx, subscription)
	if err != nil {
		return nil, fmt.Errorf("failed to get subscription %s: %w", subscription, err)
	}
	return sub, nil
}
