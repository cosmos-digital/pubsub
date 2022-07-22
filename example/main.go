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

	publisher, err := publisher.New(ctx, instance, topic.ID())
	if err != nil {
		panic(err)
	}
	defer publisher.Close()
	sub, err := subscriber.New(ctx, instance)
	if err != nil {
		panic(err)
	}

	sub.AddHandler(
		subscription.ID(),
		func(_ context.Context, msg *pubsub.Message) error {
			fmt.Println(string(msg.Data))
			return nil
		},
	)

	var wg sync.WaitGroup

	go func() {
		defer func() {
			fmt.Println("finished publishing")
		}()
		for i := 0; i < numberOfMessages; i++ {
			if err := publisher.Publish(ctx, []byte(fmt.Sprint("Hello, World! ", i))); err != nil {
				panic(err)
			}
		}
	}()

	go func() {
		if err := sub.Consume(ctx, &wg); err != nil {
			panic(err)
		}
	}()

	wg.Add(1)
	go func(sub *subscriber.Subscriber, wg *sync.WaitGroup) {
		defer wg.Done()
		time.Sleep(timeWaitForMessages)
		if err := sub.Stop(); err != nil {
			panic(err)
		}

	}(sub, &wg)

	go func() {
		defer wg.Done()
		for {
			select {
			case log := <-sub.Log():
				fmt.Println(log)
			case <-sub.Done():
				return
			default:
				continue
			}
		}
	}()
	wg.Wait()
}
