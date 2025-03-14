package main

import (
	"Anastasia/effective_mobile/msg_brokers/consumer/internal/consumer"
	consumergroup "Anastasia/effective_mobile/msg_brokers/consumer/internal/consumer_group"
	"log"

	"github.com/IBM/sarama"
)

func main() {
	addrs := []string{"localhost:9092"}
	topic := "multi-partition-topic"
	group := "test-group"
	// topic := "test-topic"

	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true

	c, err := sarama.NewConsumer(addrs, cfg)
	if err != nil {
		log.Fatal("Failed to create consumer")
	}
	defer c.Close()
	consumer.SyncRead(c, topic)

	cg, err := sarama.NewConsumerGroup(addrs, group, cfg)
	if err != nil {
		log.Fatal("Failed to create consumer group")
	}
	defer cg.Close()

	consumergroup.Read(cg, topic)
}
