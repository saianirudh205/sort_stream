package sort

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/IBM/sarama"
)


func parse(line string) Packet {
	parts := strings.Split(line, ",")
	return Packet{
		RawData:       line,
		Name:      parts[1],
		Continent: parts[3],

	}
}



func verifyTopic(brokers []string, topic string, key string) {
	cfg := sarama.NewConfig()
	cfg.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumer(brokers, cfg)
	if err != nil {
		log.Fatalf("consumer create failed: %v", err)
	}
	defer consumer.Close()

	pc, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		log.Fatalf("consume %s failed: %v", topic, err)
	}
	defer pc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	count := 0
	first20 := []Packet{}

	for {
		select {
		case msg := <-pc.Messages():
		//	fmt.Println(count)
			p := parse(string(msg.Value))

			// if count < 20 {
			// 	switch key {
			// 	case "name":
			// 		first20 = append(first20, p.Name)
			// 	case "continent":
			// 		first20 = append(first20, p.Continent)
			// 	case "id":
			// 		first20 = append(first20, fmt.SPirntln("%09d", p.ID))
			// 	}
			// }

			first20 = append(first20, p)

			count++
			if count == 20 {
				cancel()
			}

		case <-ctx.Done():
			// fmt.Println("\nTopic: %s\n", topic)
			// fmt.Println("Total packets: %d\n", count)

			if len(first20) < 2 {
				fmt.Println("FAILED: Not enough data")
				return
			}

			if verifySorted(first20, key) {
				fmt.Println("SUCCESS: first 20 messages are sorted")
			} else {
				fmt.Println("FAILED: messages are NOT sorted")
			}
			return
		}
	}
}

func test() {
	brokers := []string{"localhost:9092"}

	verifyTopic(brokers, "id", "id")
	verifyTopic(brokers, "name", "name")
	verifyTopic(brokers, "continent", "continent")
}