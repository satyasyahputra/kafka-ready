package main

import (
	"github.com/satyasyahputra/kafka-ready/consumer"
	"github.com/satyasyahputra/kafka-ready/producer"
	"github.com/segmentio/kafka-go"
)

var topic = "ex-pegasus-events"

func main() {
	messages := []kafka.Message{
		{
			Key:   []byte("Key-A"),
			Value: []byte("Hello World!"),
		},
		{
			Key:   []byte("Key-B"),
			Value: []byte("One!"),
		},
		{
			Key:   []byte("Key-C"),
			Value: []byte("Two!"),
		}}
	producer.ProduceSync(topic, messages, "hash")

	// consumer.Consume([]string{"localhost:9092"}, topic, "consumer-group-id-v1")
	consumer.ConsumerGroup([]string{"localhost:9092"}, topic, "consumer-group-id-v1")
}
