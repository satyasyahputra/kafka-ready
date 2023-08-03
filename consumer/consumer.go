package consumer

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
)

func CreateReader(brokers []string, topic string, groupID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		GroupID:  groupID,
		Topic:    topic,
		MaxBytes: 10e6, // 10MB
	})
}

func Consume(brokers []string, topic string, groupID string) {
	r := CreateReader(brokers, topic, groupID)

	// for {
	// 	m, err := r.ReadMessage(context.Background())
	// 	if err != nil {
	// 		break
	// 	}
	// 	fmt.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
	// }

	// if err := r.Close(); err != nil {
	// 	log.Fatal("failed to close reader:", err)
	// }

	m, err := r.ReadMessage(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
	r.Close()
}

func ConsumerGroup(brokers []string, topic string, groupID string) {
	group, err := kafka.NewConsumerGroup(kafka.ConsumerGroupConfig{
		ID:      groupID,
		Brokers: brokers,
		Topics:  []string{topic},
	})

	if err != nil {
		os.Exit(1)
	}

	defer group.Close()
	for {
		gen, err := group.Next(context.TODO())
		if err != nil {
			break
		}

		assignments := gen.Assignments[topic]
		for _, assignment := range assignments {
			partition, offset := assignment.ID, assignment.Offset
			gen.Start(func(ctx context.Context) {
				// create reader for this partition.
				reader := kafka.NewReader(kafka.ReaderConfig{
					Brokers:        brokers,
					Topic:          topic,
					Partition:      partition,
					CommitInterval: time.Second,
				})
				defer reader.Close()

				// seek to the last committed offset for this partition.
				log.Printf("partition: %d - offset: %d", partition, offset)
				reader.SetOffset(offset)
				for {
					msg, err := reader.ReadMessage(ctx)
					if err != nil {
						if errors.Is(err, kafka.ErrGenerationEnded) {
							// generation has ended.  commit offsets.  in a real app,
							// offsets would be committed periodically.
							gen.CommitOffsets(map[string]map[int]int64{topic: {partition: offset + 1}})
							return
						}

						fmt.Printf("error reading message: %+v\n", err)
						return
					}

					fmt.Printf("received message %s/%d/%d : %s\n", msg.Topic, msg.Partition, msg.Offset, string(msg.Value))
					offset = msg.Offset
				}
			})
		}
	}
}
