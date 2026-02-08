package sort

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
)

// ---------------------------------------------------------------------------
// Consumer Configuration
// ---------------------------------------------------------------------------

type ConsumerConfig struct {
	Partition      int
	Brokers        []string
	SourceTopic    string
	TempDir        string
	HeapFlushSize  int
	IdleTimeout    time.Duration
	MaxMessages    int64 // 0 = unlimited
	RetryConfig    RetryConfig
}

// ---------------------------------------------------------------------------
// Partition consumer with retry logic
// ---------------------------------------------------------------------------

func consumePartition(ctx context.Context, partition int, brokers []string,
	sourceTopic string, tempDir string, heapFlushSize int, idleTimeout time.Duration) (int, error) {
	
	config := ConsumerConfig{
		Partition:     partition,
		Brokers:       brokers,
		SourceTopic:   sourceTopic,
		TempDir:       tempDir,
		HeapFlushSize: heapFlushSize,
		IdleTimeout:   idleTimeout,
		MaxMessages:   0, // unlimited
		RetryConfig:   defaultRetryConfig,
	}
	
	return consumePartitionWithRetry(ctx, config)
}

func consumePartitionWithRetry(ctx context.Context, cfg ConsumerConfig) (int, error) {
	// Initialize heap writer with retry
	var hw *HeapWriter
	err := retryWithBackoff(ctx, cfg.RetryConfig, fmt.Sprintf("create heap writer P%d", cfg.Partition), func() error {
		var err error
		hw, err = NewHeapWriter(cfg.Partition, cfg.TempDir, cfg.HeapFlushSize)
		return err
	})
	if err != nil {
		return 0, fmt.Errorf("failed to create heap writer after retries: %w", err)
	}
	defer hw.Close()

	// Create Sarama config
	saramaConfig := sarama.NewConfig()
	saramaConfig.Consumer.Return.Errors = true
	saramaConfig.Consumer.Fetch.Default = 10 * 1024 * 1024
	saramaConfig.Consumer.Retry.Backoff = cfg.RetryConfig.InitialBackoff
	saramaConfig.Consumer.MaxProcessingTime = 30 * time.Second

	// Create consumer with retry
	var consumer sarama.Consumer
	err = retryWithBackoff(ctx, cfg.RetryConfig, fmt.Sprintf("create consumer P%d", cfg.Partition), func() error {
		var err error
		consumer, err = sarama.NewConsumer(cfg.Brokers, saramaConfig)
		return err
	})
	if err != nil {
		return 0, fmt.Errorf("failed to create consumer after retries: %w", err)
	}
	defer consumer.Close()

	// Create partition consumer with retry
	var pc sarama.PartitionConsumer
	err = retryWithBackoff(ctx, cfg.RetryConfig, fmt.Sprintf("consume partition P%d", cfg.Partition), func() error {
		var err error
		pc, err = consumer.ConsumePartition(cfg.SourceTopic, int32(cfg.Partition), sarama.OffsetOldest)
		return err
	})
	if err != nil {
		return 0, fmt.Errorf("failed to consume partition %d after retries: %w", cfg.Partition, err)
	}
	defer pc.Close()

	// Start consumption
	return runConsumptionLoop(ctx, cfg, hw, pc)
}

// ---------------------------------------------------------------------------
// Main consumption loop
// ---------------------------------------------------------------------------

func runConsumptionLoop(ctx context.Context, cfg ConsumerConfig, hw *HeapWriter, pc sarama.PartitionConsumer) (int, error) {
	count := 0
	parseErrors := 0
	writeErrors := 0
	consumerErrors := 0
	lastMsgTime := time.Now()
	
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	log.Printf("[P%d] consuming from topic=%s...", cfg.Partition, cfg.SourceTopic)

	for {
		select {
		case <-ctx.Done():
			log.Printf("[P%d] context cancelled: total=%d written=%d parseErrors=%d writeErrors=%d consumerErrors=%d",
				cfg.Partition, count, hw.GetTotalWritten(), parseErrors, writeErrors, consumerErrors)
			return count, ctx.Err()

		case msg := <-pc.Messages():
			if msg == nil {
				log.Printf("[P%d] received nil message, continuing", cfg.Partition)
				continue
			}

			// Parse packet with retry
			var p Packet
			parseErr := retryWithBackoff(ctx, cfg.RetryConfig, fmt.Sprintf("parse packet P%d offset=%d", cfg.Partition, msg.Offset), func() error {
				var err error
				p, err = parsePacket(string(msg.Value))
				return err
			})

			if parseErr != nil {
				parseErrors++
				log.Printf("[P%d] CRITICAL: failed to parse message after retries offset=%d: %v", cfg.Partition, msg.Offset, parseErr)
				
				// In production, you might want to:
				// 1. Send to Dead Letter Queue (DLQ)
				// 2. Store in separate error file
				// 3. Alert monitoring system
				
				// For now, we continue to avoid blocking the entire partition
				if parseErrors%100 == 0 {
					log.Printf("[P%d] WARNING: %d parse errors so far", cfg.Partition, parseErrors)
				}
				continue
			}

			// Write packet with retry
			writeErr := retryWithBackoff(ctx, cfg.RetryConfig, fmt.Sprintf("write packet P%d", cfg.Partition), func() error {
				return hw.AddPacket(p)
			})

			if writeErr != nil {
				writeErrors++
				log.Printf("[P%d] CRITICAL: failed to write packet after retries: %v", cfg.Partition, writeErr)
				
				// Write errors are more serious - might indicate disk full, corruption, etc.
				// Return error to stop this partition consumer
				return count, fmt.Errorf("P%d write error after retries: %w", cfg.Partition, writeErr)
			}

			count++
			lastMsgTime = time.Now()

			// Progress logging
			if count%1000000 == 0 {
				log.Printf("[P%d] progress: %dM messages (%.0f msg/s, parseErrors=%d, writeErrors=%d, consumerErrors=%d)",
					cfg.Partition,
					count/1000000,
					float64(count)/time.Since(lastMsgTime).Seconds(),
					parseErrors,
					writeErrors,
					consumerErrors,
				)
			}

			// Check max messages limit (if configured)
			if cfg.MaxMessages > 0 && int64(count) >= cfg.MaxMessages {
				log.Printf("[P%d] reached max messages limit: total=%d written=%d", cfg.Partition, count, hw.GetTotalWritten())
				return count, nil
			}

		case err := <-pc.Errors():
			if err == nil {
				continue
			}
			
			consumerErrors++
			log.Printf("[P%d] consumer error #%d: %v", cfg.Partition, consumerErrors, err)

			// Attempt to recover from consumer errors
			if consumerErrors >= cfg.RetryConfig.MaxAttempts {
				log.Printf("[P%d] CRITICAL: too many consumer errors (%d), stopping partition consumer", cfg.Partition, consumerErrors)
				return count, fmt.Errorf("P%d: too many consumer errors: %w", cfg.Partition, err)
			}

			// For transient errors, log and continue
			// For serious errors (network partition, etc.), the retry count will accumulate
			if consumerErrors%10 == 0 {
				log.Printf("[P%d] WARNING: %d consumer errors, may need intervention", cfg.Partition, consumerErrors)
			}

		case <-ticker.C:
			elapsed := time.Since(lastMsgTime)
			
			// Idle timeout check
			if elapsed > cfg.IdleTimeout || count == 12500000 {
				log.Printf("[P%d] idle timeout (%.1fs > %.1fs): total=%d written=%d parseErrors=%d writeErrors=%d consumerErrors=%d",
					cfg.Partition,
					elapsed.Seconds(),
					cfg.IdleTimeout.Seconds(),
					count,
					hw.GetTotalWritten(),
					parseErrors,
					writeErrors,
					consumerErrors,
				)
				return count, nil
			}

			// Periodic health check
			
			if count > 0 {
				log.Printf("[P%d] health check: alive=%t lastMsg=%.1fs_ago total=%d errors=(parse:%d write:%d consumer:%d)",
					cfg.Partition,
					true,
					elapsed.Seconds(),
					count,
					parseErrors,
					writeErrors,
					consumerErrors,
				)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Helper: Consume with custom retry configuration
// ---------------------------------------------------------------------------

func ConsumePartitionWithConfig(ctx context.Context, cfg ConsumerConfig) (int, error) {
	return consumePartitionWithRetry(ctx, cfg)
}