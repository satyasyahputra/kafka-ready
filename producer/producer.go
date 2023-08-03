package producer

import (
	"context"
	"errors"

	"github.com/segmentio/kafka-go"
)

func GetBalancer(balancer string) kafka.Balancer {
	balancerMap := map[string]kafka.Balancer{
		"hash":           &kafka.Hash{},
		"random":         &kafka.RoundRobin{},
		"reference_hash": &kafka.ReferenceHash{},
		"crc32":          &kafka.CRC32Balancer{},
		"murmur":         &kafka.Murmur2Balancer{},
	}
	return balancerMap[balancer]
}

func ProduceSync(topic string, messages []kafka.Message, balancer string) error {

	if len(messages) == 0 {
		return errors.New("empty messages")
	}

	if topic == "" {
		return errors.New("empty topic")
	}

	w := &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    topic,
		Balancer: GetBalancer(balancer),
	}

	err := w.WriteMessages(context.Background(),
		messages...,
	)
	if err != nil {
		return errors.New("failed to write messages")
	}

	if err := w.Close(); err != nil {
		return errors.New("failed to close writer")
	}
	return nil
}
