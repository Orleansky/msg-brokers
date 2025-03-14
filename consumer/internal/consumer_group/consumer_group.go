package consumergroup

import (
	"context"
	"log"

	"github.com/IBM/sarama"
)

func Read(cg sarama.ConsumerGroup, topic string) {
	go func() {
		for err := range cg.Errors() {
			log.Fatalf("Error in consumer group: %v", err)
		}
	}()

	handler := &ConsumerGroupHandler{}
	ctx := context.Background()
	for {
		err := cg.Consume(ctx, []string{topic}, handler)
		if err != nil {
			log.Fatalf("Consuming error: %v", err)
		}
	}
}

type ConsumerGroupHandler struct{}

func (h *ConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	log.Println("Setting consumer up")
	return nil
}

func (h *ConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	log.Println("Finishing consuming")
	return nil
}

func (h *ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		log.Printf("Got message: Topic = %s, Partition = %d, Key = %s, Value = %s\n",
			msg.Topic, msg.Partition, string(msg.Key), string(msg.Value))
		session.MarkMessage(msg, "")
	}
	return nil
}
