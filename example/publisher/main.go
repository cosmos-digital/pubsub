package main

import (
	"context"
	"os"

	"github.com/cosmos-digital/pubsub/publisher"
)

func main() {
	os.Setenv("PUBSUB_EMULATOR_HOST", "localhost:8538")
	ctx := context.Background()
	publisher, err := publisher.New(ctx, "project-test", "topic-test")
	if err != nil {
		panic(err)
	}
	defer publisher.Close()

	if err := publisher.Publish(ctx, []byte("Hello, World!")); err != nil {
		panic(err)
	}
}
