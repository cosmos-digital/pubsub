package main

import (
	"context"
	"os"

	pubsub "bitbucket.org/cosmos-digital/pubsub"
)

func main() {
	os.Setenv("PUBSUB_EMULATOR_HOST", "localhost:8538")
	ctx := context.Background()
	publisher, err := pubsub.NewPublisher(ctx, "project-test", "topic-test")
	if err != nil {
		panic(err)
	}
	defer publisher.Close()

	if err := publisher.Publish(ctx, []byte("Hello, World!")); err != nil {
		panic(err)
	}
}
