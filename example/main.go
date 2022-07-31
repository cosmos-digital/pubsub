package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/cosmos-digital/pubsub"
	"github.com/cosmos-digital/pubsub/publisher"
	"github.com/cosmos-digital/pubsub/subscriber"
	"github.com/sirupsen/logrus"
)

var (
	emulatorHost        = "localhost:8538"
	projectID           = "project-test"
	subscriptionID      = "test-subscription"
	topicID             = "test-topic"
	numberOfMessages    = 10
	timeWaitForMessages = time.Second * 5
)

func main() {

	os.Setenv("PUBSUB_EMULATOR_HOST", emulatorHost)

	ctx := context.Background()

	instance, err := pubsub.New(ctx, projectID)
	if err != nil {
		panic(err)
	}
	defer instance.Close()

	topic, err := instance.GetTopic(ctx, topicID)
	if err != nil {
		if errors.Is(err, pubsub.ErrTopicNotFound{}) {
			topic, err = instance.CreateTopic(ctx, topicID)
			if err != nil {
				panic(err)
			}
			fmt.Println("created topic:", topic.ID())
		} else {
			panic(err)
		}
	}

	subscription, err := instance.GetSubscription(ctx, subscriptionID)
	if err != nil {
		if errors.Is(err, pubsub.ErrSubscriptionNotFound{}) {
			subscription, err = instance.CreateSubscription(ctx, subscriptionID, topic)
			if err != nil {
				panic(err)
			}
			fmt.Println("created subscription:", subscription.ID())
		} else {
			panic(err)
		}
	}

	publisher, err := publisher.New(ctx, instance)
	if err != nil {
		panic(err)
	}
	defer publisher.Close()
	sub, err := subscriber.New(ctx, instance)
	if err != nil {
		panic(err)
	}

	sub.AddConsumer(
		subscription.ID(),
		func(ctx context.Context, msg *pubsub.Message) error {
			fmt.Println(string(msg.Data))
			return nil
		},
	)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer func() {
			fmt.Println("finished publishing")
			wg.Done()
		}()
		for i := 0; i < numberOfMessages; i++ {
			if err := publisher.Publish(ctx, topic, &pubsub.Message{
				Data: []byte(fmt.Sprint("Hello, World! ", i)),
			},
			); err != nil {
				panic(err)
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(timeWaitForMessages)
		if err := sub.Stop(); err != nil {
			panic(err)
		}
	}()
	if sub.Start() != nil {
		panic(err)
	}

	go func() {
		for _, ch := range sub.Channels() {
			for {
				select {
				case log := <-ch:
					if log.Err != nil {
						logrus.WithError(log.Err).Error(log.Consumer)
					}
				case <-ctx.Done():
					fmt.Println("context done")
					return
				}
			}
		}
	}()
	wg.Wait()
}
