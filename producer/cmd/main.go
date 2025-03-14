package main

import (
	"Anastasia/effective_mobile/msg_brokers/producer/internal/producer"
	"log"

	"github.com/IBM/sarama"
)

func main() {
	addrs := []string{"localhost:9092"}
	topic := "test-topic"

	cfg := sarama.NewConfig()
	cfg.Consumer.Offsets.AutoCommit.Enable = true
	cfg.Producer.Return.Successes = true

	topic = "multi-partition-topic"
	var partitionCount int32 = 3

	p, err := sarama.NewSyncProducer(addrs, cfg)
	if err != nil {
		log.Printf("Error creating producer: %v", err)
	}

	// p, err := sarama.NewAsyncProducer(addrs, cfg)
	// if err != nil {
	// 	log.Printf("Error creating producer: %v", err)
	// }

	defer p.Close()

	admin, err := sarama.NewClusterAdmin(addrs, cfg)
	if err != nil {
		log.Printf("Error creating cluster admin: %v", err)
	}

	defer admin.Close()

	// Выбрать один метод для проверки
	producer.MultiPartitionSend(admin, p, topic, partitionCount)
	// producer.SyncSend(p, topic)
	// producer.AsyncSend(p, topic)
}
