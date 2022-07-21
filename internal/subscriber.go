package pubsub

import (
	"context"
	"fmt"
	"sync"

	"cloud.google.com/go/pubsub"
)

type Subscriber struct {
	instance *Instance
	handler  map[string]Handler
	log      chan string
	done     chan bool
}

func (s *Subscriber) AddHandler(subscription string, handler Handler) *Subscriber {
	s.handler[subscription] = handler
	return s
}

func (s *Subscriber) Consume(ctx context.Context) error {
	var wg sync.WaitGroup
	for subscriptionName, handler := range s.handler {
		subscription, err := s.instance.GetSubscription(ctx, subscriptionName)
		if err != nil {
			return fmt.Errorf("failed to get subscription %s: %w", subscription, err)
		}
		wg.Add(1)
		go func(ctx context.Context, wg sync.WaitGroup, subscription *pubsub.Subscription, handler Handler) {
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
	wg.Add(1)
	go func(wg sync.WaitGroup) {
		defer wg.Done()
		for {
			select {
			case msg := <-s.log:
				fmt.Println(msg)
			case <-s.done:
				fmt.Println("done")
				wg.Done()
				return
			}
		}
	}(wg)
	wg.Wait()
	return nil
}

func (s *Subscriber) Stop() error {
	if err := s.instance.Close(); err != nil {
		return fmt.Errorf("failed to close client: %w", err)
	}
	s.done <- true
	return nil
}

func (s *Subscriber) Log() <-chan string {
	return s.log
}

func (s *Subscriber) Close() error {
	err := s.instance.Close()
	if err != nil {
		return fmt.Errorf("failed to close client: %w", err)
	}
	return nil
}
