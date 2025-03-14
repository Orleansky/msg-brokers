package consumer

import (
	"log"
	"sync"

	"github.com/IBM/sarama"
)

func SyncRead(c sarama.Consumer, topic string) {
	wg := new(sync.WaitGroup)
	partitions, err := c.Partitions(topic)
	if err != nil {
		log.Printf("Failed to get partitions: %v", err)
	}

	for _, partition := range partitions {
		pc, err := c.ConsumePartition(topic, partition, sarama.OffsetNewest)
		if err != nil {
			log.Fatalf("Failed to start consumer for partition %d: %v\n", partition, err)
		}
		defer pc.Close()

		wg.Add(1)
		go func(pc sarama.PartitionConsumer) {
			defer wg.Done()
			for msg := range pc.Messages() {
				log.Printf("Partition: %d Key: %s Message: %s\n", partition, msg.Key, msg.Value)
			}
		}(pc)
	}
	wg.Wait()
}

func AsyncRead(c sarama.Consumer, topic string) {
	partitionList, err := c.Partitions(topic)
	if err != nil {
		log.Printf("Failed to get partitions: %v", err)
	}

	var wg sync.WaitGroup
	for _, partition := range partitionList {
		wg.Add(1)
		go func(partition int32) {
			defer wg.Done()
			partitionConsumer, err := c.ConsumePartition(topic, partition, sarama.OffsetNewest)
			if err != nil {
				log.Fatalf("Failed to create partition consumer: %v", err)
			}
			defer partitionConsumer.Close()

			for msg := range partitionConsumer.Messages() {
				log.Printf("Partition: %d Key: %s Message: %s\n", partition, msg.Key, msg.Value)
			}
		}(partition)
	}
	wg.Wait()
}
