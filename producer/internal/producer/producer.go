package producer

import (
	"fmt"
	"log"

	"github.com/IBM/sarama"
)

func SyncSend(producer sarama.SyncProducer, topic string) {
	for i := range 10 {
		key := fmt.Sprintf("Key-%d", i)
		msg := fmt.Sprintf("Message №%d", i)
		producerMsg := &sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.StringEncoder(key),
			Value: sarama.StringEncoder(msg),
		}

		if _, _, err := producer.SendMessage(producerMsg); err != nil {
			log.Printf("Error sending message: %v", err)
		}
	}
}

func AsyncSend(producer sarama.AsyncProducer, topic string) {
	for i := range 10 {
		key := fmt.Sprintf("Key-%d", i)
		msg := fmt.Sprintf("Message №%d", i)
		producerMsg := &sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.StringEncoder(key),
			Value: sarama.StringEncoder(msg),
		}
		producer.Input() <- producerMsg
	}

	for range 10 {
		select {
		case success := <-producer.Successes():
			log.Printf("Message sent: %v\n", success.Value)
		case err := <-producer.Errors():
			log.Printf("Error sending message: %v", err)
		}
	}
}

func MultiPartitionSend(admin sarama.ClusterAdmin, producer sarama.SyncProducer, topic string, partitionCount int32) {
	topics, err := admin.ListTopics()
	if err != nil {
		log.Fatalf("Error listing topics: %v", err)
	}

	if topicDetail, exists := topics[topic]; exists {
		currentPartitions := topicDetail.NumPartitions
		log.Printf("Topic '%s' already exists with %d partitions\n", topic, currentPartitions)

		if currentPartitions < partitionCount {
			if err := admin.CreatePartitions(topic, partitionCount, nil, false); err != nil {
				log.Fatalf("Error creating partitions: %v", err)
			}
			log.Printf("Increased partitions for topic '%s' to %d\n", topic, partitionCount)
		} else {
			log.Printf("Topic '%s' already has %d partitions (no changes needed)\n", topic, currentPartitions)
		}
	} else {
		topicDetail := &sarama.TopicDetail{
			NumPartitions:     partitionCount,
			ReplicationFactor: 1,
		}
		if err := admin.CreateTopic(topic, topicDetail, false); err != nil {
			log.Fatalf("Error creating topic: %v", err)
		}
		log.Printf("Created topic '%s' with %d partitions\n", topic, partitionCount)
	}

	for i := int32(0); i < partitionCount; i++ {
		msg := &sarama.ProducerMessage{
			Topic:     topic,
			Partition: i,
			Key:       sarama.StringEncoder(fmt.Sprintf("Key %d", i)),
			Value:     sarama.StringEncoder(fmt.Sprintf("Message %d", i)),
		}
		if _, _, err := producer.SendMessage(msg); err != nil {
			log.Fatalf("Error sending message: %v", err)
		}
		log.Printf("Message sent to partition %d\n", i)
	}
}
