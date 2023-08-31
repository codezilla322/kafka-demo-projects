package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers"	: "localhost:9092",
		"client.id"         : "kafka-demo",
		"security.protocol" : "SASL_PLAINTEXT",
		"sasl.mechanism"    : "PLAIN",
		"sasl.username"     : "test",
		"sasl.password"     : "test-secret",
		"group.id"					: "kafka-group",
	})

	if err != nil {
		fmt.Printf("Failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	err = c.Subscribe("test-topic", nil)

	run := true

	for run {
		select {
			case sig := <-sigchan:
				fmt.Printf("Caught signal %v: terminating\n", sig)
				run = false
			default:
				ev := c.Poll(100)
				if ev == nil {
					continue
				}

				switch e := ev.(type) {
					case *kafka.Message:
						fmt.Printf("%% Message on %s:\n%s\n", e.TopicPartition, string(e.Value))
						if e.Headers != nil {
							fmt.Printf("%% Headers: %v\n", e.Headers)
						}
					case kafka.Error:
						fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n", e.Code(), e)
						if e.Code() == kafka.ErrAllBrokersDown {
							run = false
						}
					default:
						fmt.Printf("Ignored %v\n", e)
				}
		}
	}

	c.Close()
}
