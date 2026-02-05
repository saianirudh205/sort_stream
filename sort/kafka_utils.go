package sort

import (
	"log"

	"github.com/IBM/sarama"
)

// ---------------------------------------------------------------------------
// Topic helper
// ---------------------------------------------------------------------------

func createOutputTopics(brokers []string, topics []string) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_6_0_0

	admin, err := sarama.NewClusterAdmin(brokers, config)
	if err != nil {
		log.Fatalf("admin connect: %v", err)
	}
	defer admin.Close()

	for _, t := range topics {
		err := admin.CreateTopic(t, &sarama.TopicDetail{
			NumPartitions:     1,
			ReplicationFactor: 1,
		}, false)
		if err != nil {
			log.Printf("topic %s: %v (may already exist)", t, err)
		} else {
			log.Printf("created topic: %s", t)
		}
	}
}
