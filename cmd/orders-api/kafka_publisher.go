package main

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/segmentio/kafka-go"
)

type kafkaPublisher struct {
	writer *kafka.Writer
}

func newKafkaPublisher(brokers []string, topic string) (*kafkaPublisher, error) {
	if len(brokers) == 0 {
		return nil, errors.New("at least one Kafka broker is required")
	}
	if topic == "" {
		return nil, errors.New("Kafka topic is required")
	}

	return &kafkaPublisher{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        topic,
			Balancer:     &kafka.Hash{},
			RequiredAcks: kafka.RequireAll,
		},
	}, nil
}

func (p *kafkaPublisher) Publish(ctx context.Context, event orderCreatedEvent) error {
	value, err := json.Marshal(event)
	if err != nil {
		return err
	}

	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(event.OrderID),
		Value: value,
	})
}

func (p *kafkaPublisher) Close() error {
	return p.writer.Close()
}
