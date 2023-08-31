package main

import (
	"fmt"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers"	: "localhost:9092",
		"client.id"         : "kafka-demo",
		"security.protocol" : "SASL_PLAINTEXT",
		"sasl.mechanism"    : "PLAIN",
		"sasl.username"     : "test",
		"sasl.password"     : "test-secret",
	})

	if err != nil {
		fmt.Printf("Failed to create Admin client: %s\n", err)
		os.Exit(1)
	}

	topic := new(string)
	*topic = "test-topic"

	metaData, err := p.GetMetadata(topic, false, 60 * 1000)

	if err != nil {
		fmt.Printf("Failed to get topic metadata: %v\n", err)
		os.Exit(1)
	}

	topicMetaData := metaData.Topics[*topic]
	fmt.Printf("%s\n", topicMetaData.Topic)

	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				m := ev
				if m.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
				} else {
					fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
						*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
				}
			case kafka.Error:
				fmt.Printf("Error: %v\n", ev)
			default:
				fmt.Printf("Ignored event: %s\n", ev)
			}
		}
	}()

	msgcnt := 0
	for msgcnt < 3 {
		value := fmt.Sprintf("Producer example, message #%d", msgcnt)

		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: topic, Partition: kafka.PartitionAny},
			Value					: []byte(value),
			Key						: []byte("key1"),
		}, nil)

		if err != nil {
			if err.(kafka.Error).Code() == kafka.ErrQueueFull {
				time.Sleep(time.Second)
				continue
			}
			fmt.Printf("Failed to produce message: %v\n", err)
		}
		msgcnt++
	}

	for p.Flush(5000) > 0 {
		fmt.Print("Still waiting to flush outstanding messages\n")
	}

	p.Close();
}