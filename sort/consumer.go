package sort

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
)

// ---------------------------------------------------------------------------
// Partition consumer
// ---------------------------------------------------------------------------

func consumePartition(ctx context.Context, partition int, brokers []string,
	sourceTopic string, tempDir string, heapFlushSize int, idleTimeout time.Duration) (int, error) {

	hw, err := NewHeapWriter(partition, tempDir, heapFlushSize)
	if err != nil {
		return 0, err
	}

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Fetch.Default = 10 * 1024 * 1024

	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		hw.Close()
		return 0, fmt.Errorf("error creating consumer: %v", err)
	}
	defer consumer.Close()

	pc, err := consumer.ConsumePartition(sourceTopic, int32(partition), sarama.OffsetOldest)
	if err != nil {
		hw.Close()
		return 0, fmt.Errorf("error consuming partition %d: %v", partition, err)
	}
	defer pc.Close()

	count := 0
	lastMsgTime := time.Now()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	log.Printf("[P%d] consuming...", partition)

	for {
		select {
		case <-ctx.Done():
			hw.Close()
			log.Printf("[P%d] done (ctx) total=%d written=%d", partition, count, hw.GetTotalWritten())
			return count, nil

		case msg := <-pc.Messages():
			p, err := parsePacket(string(msg.Value))
			if err != nil {
				log.Printf("[P%d] parse error: %v", partition, err)
				continue
			}
			if err := hw.AddPacket(p); err != nil {
				hw.Close()
				return count, fmt.Errorf("P%d write error: %v", partition, err)
			}
			count++
			lastMsgTime = time.Now()
			if count%1000000 == 0 {
				log.Printf("[P%d] %d messages", partition, count)
			}

			if count == 12500000{
				hw.Close()
				log.Printf("[P%d] idle timeout total=%d written=%d", partition, count, hw.GetTotalWritten())
				return count, nil
			}

		case err := <-pc.Errors():
			log.Printf("[P%d] consumer error: %v", partition, err)

		case <-ticker.C:
			if time.Since(lastMsgTime) > idleTimeout {
				hw.Close()
				log.Printf("[P%d] idle timeout total=%d written=%d", partition, count, hw.GetTotalWritten())
				return count, nil
			}
		}
	}
}
