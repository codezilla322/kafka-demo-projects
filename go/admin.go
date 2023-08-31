package main

import (
	"context"
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {

	a, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers"	: "localhost:9092",
		"client.id"         : "kafka-demo",
		"security.protocol" : "SASL_PLAINTEXT",
		"sasl.mechanism"    : "PLAIN",
		"sasl.username"     : "admin",
		"sasl.password"     : "admin-secret",
	})

	if err != nil {
		fmt.Printf("Failed to create Admin client: %s\n", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	results, err := a.CreateTopics(
		ctx,
		[]kafka.TopicSpecification{{
			Topic					: "test-topic",
			NumPartitions	: 2}})

	if err != nil {
		fmt.Printf("Failed to create topic: %v\n", err)
		os.Exit(1)
	}

	for _, result := range results {
		fmt.Printf("%s\n", result)
	}

	a.Close()
}