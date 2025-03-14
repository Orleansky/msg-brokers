package main

import (
	"Anastasia/effective_mobile/msg_brokers/consumer/internal/consumer"
	"log"

	"github.com/IBM/sarama"
)

func main() {
	addrs := []string{"localhost:9092"}
	// topic := "test-topic"
	topic := "multi-partition-topic"
	group := "test-group"

	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true

	c, err := sarama.NewConsumer(addrs, cfg)
	if err != nil {
		log.Fatal("Failed to create consumer")
	}
	defer c.Close()

	cg, err := sarama.NewConsumerGroup(addrs, group, cfg)
	if err != nil {
		log.Fatal("Failed to create consumer group")
	}
	defer cg.Close()

	// Выбрать один метод для проверки
	consumer.SyncRead(c, topic)
	// consumer.AsyncRead(c, topic)
	// consumergroup.Read(cg, topic)
}
