package main

import (
	"context"
	"fmt"
	"os"

	pubsub "github.com/cosmos-digital/pubsub"
)

func main() {
	os.Setenv("PUBSUB_EMULATOR_HOST", "localhost:8538")
	ctx := context.Background()
	subscriber, err := pubsub.NewSubscriber(ctx, "project-test")
	if err != nil {
		panic(err)
	}
	defer subscriber.Close()

	subscriber.AddHandler(
		"subscription-test",
		func(_ context.Context, msg pubsub.Message) error {
			fmt.Println(string(msg.Data))
			return nil
		},
	)
	if err := subscriber.Consume(ctx); err != nil {
		panic(err)
	}
}
