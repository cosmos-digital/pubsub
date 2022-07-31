package subscriber

import (
	"context"
	"fmt"

	pubsub "github.com/cosmos-digital/pubsub/internal"
)

type logger struct {
	Consumer string
	Err      error
}
type consumer struct {
	handler pubsub.Handler
	done    chan bool
}

type Client struct {
	instance  *pubsub.Instance
	consumers map[string]consumer
	channels  map[string]chan logger
	ctx       context.Context
	done      chan bool
}

func New(ctx context.Context, instance *pubsub.Instance) (*Client, error) {
	return &Client{
		instance:  instance,
		consumers: make(map[string]consumer),
		done:      make(chan bool),
		ctx:       ctx,
	}, nil
}

func (s *Client) AddConsumer(subscription string, handler pubsub.Handler) *Client {
	s.consumers[subscription] = consumer{
		handler: handler,
		done:    make(chan bool),
	}
	return s
}

func (s *Client) Start() error {
	s.channels = make(map[string]chan logger, len(s.consumers))
	for subscription, consumer := range s.consumers {
		subscription, err := s.instance.GetSubscription(s.ctx, subscription)
		if err != nil {
			return fmt.Errorf("failed to get subscription %s: %w", subscription, err)
		}

		channel := make(chan logger)
		s.channels[subscription.ID()] = channel

		go func(subscription *pubsub.Subscription, handler pubsub.Handler) {
			logger := logger{
				Consumer: subscription.ID(),
			}
			err = subscription.Receive(s.ctx, func(ctx context.Context, message *pubsub.Message) {
				if err = handler(ctx, message); err != nil {
					logger.Err = err
					channel <- logger
					message.Nack()
				}
				message.Ack()
			})
			if err != nil {
				logger.Err = err
				channel <- logger
			}
		}(subscription, consumer.handler)

	}
	return nil
}

func (c *Client) Channels() map[string]chan logger {
	return c.channels
}

func (s *Client) Loggers() {
	for _, channel := range s.channels {
		go func(ch <-chan logger) {
			for log := range ch {
				fmt.Println("consumer channel closed", log.Err.Error())
			}
		}(channel)
	}
}

func (s *Client) Stop() error {
	_, ok := <-s.done
	if !ok {
		return fmt.Errorf("client already stopped")
	}
	s.done <- true
	return nil

}

func (s *Client) Close() error {
	err := s.instance.Close()
	if err != nil {
		return fmt.Errorf("failed to close client: %w", err)
	}
	return nil
}

func (s *Client) GetSubscription(ctx context.Context, subscription string) (*pubsub.Subscription, error) {
	sub, err := s.instance.GetSubscription(ctx, subscription)
	if err != nil {
		return nil, fmt.Errorf("failed to get subscription %s: %w", subscription, err)
	}
	return sub, nil
}
