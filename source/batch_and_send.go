package source

import (
	"context"
	"flag"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

var (
	broker  = flag.String("broker", "localhost:9092", "Kafka broker")
	topic   = flag.String("topic", "source", "Kafka topic")
	total   = flag.Int64("total", 50_000_000, "Total records to generate")
	workers = flag.Int("workers", 4, "Number of workers")
	batch   = flag.Int("batch", 2000, "Kafka batch size")
)

var (
	letters      = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	addressChars = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 ")
	continents   = []string{
		"North America", "Asia", "South America",
		"Europe", "Africa", "Australia",
	}
)

// ---------------------------------------------------------------------------
// Retry Configuration (reusing from sort package pattern)
// ---------------------------------------------------------------------------

type RetryConfig struct {
	MaxAttempts     int
	InitialBackoff  time.Duration
	MaxBackoff      time.Duration
	BackoffMultiple float64
}

var defaultRetryConfig = RetryConfig{
	MaxAttempts:     5,
	InitialBackoff:  100 * time.Millisecond,
	MaxBackoff:      30 * time.Second,
	BackoffMultiple: 2.0,
}

// ---------------------------------------------------------------------------
// Producer Configuration
// ---------------------------------------------------------------------------

type ProducerConfig struct {
	Broker         string
	Topic          string
	TotalRecords   int64
	Workers        int
	BatchSize      int
	RetryConfig    RetryConfig
	ShutdownWindow time.Duration // Grace period for shutdown
}

func DefaultProducerConfig() ProducerConfig {
	return ProducerConfig{
		Broker:         "localhost:9092",
		Topic:          "source",
		TotalRecords:   50_000_000,
		Workers:        4,
		BatchSize:      2000,
		RetryConfig:    defaultRetryConfig,
		ShutdownWindow: 30 * time.Second,
	}
}

// ---------------------------------------------------------------------------
// Statistics tracking
// ---------------------------------------------------------------------------

type ProducerStats struct {
	sent          int64
	writeErrors   int64
	retriedWrites int64
	workerErrors  int64
}

func (s *ProducerStats) recordSent(count int64) {
	atomic.AddInt64(&s.sent, count)
}

func (s *ProducerStats) recordWriteError() {
	atomic.AddInt64(&s.writeErrors, 1)
}

func (s *ProducerStats) recordRetry() {
	atomic.AddInt64(&s.retriedWrites, 1)
}

func (s *ProducerStats) recordWorkerError() {
	atomic.AddInt64(&s.workerErrors, 1)
}

func (s *ProducerStats) snapshot() (sent, writeErrors, retries, workerErrors int64) {
	return atomic.LoadInt64(&s.sent),
		atomic.LoadInt64(&s.writeErrors),
		atomic.LoadInt64(&s.retriedWrites),
		atomic.LoadInt64(&s.workerErrors)
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

func randFromSet(r *rand.Rand, set []byte, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = set[r.Intn(len(set))]
	}
	return b
}

func calculateBackoff(attempt int, config RetryConfig) time.Duration {
	backoff := float64(config.InitialBackoff)
	for i := 0; i < attempt; i++ {
		backoff *= config.BackoffMultiple
		if backoff > float64(config.MaxBackoff) {
			backoff = float64(config.MaxBackoff)
			break
		}
	}
	// Add jitter: 80-100% of calculated backoff
	jitter := 0.8 + (0.2 * float64(time.Now().UnixNano()%100) / 100.0)
	return time.Duration(backoff * jitter)
}

// ---------------------------------------------------------------------------
// Resilient Kafka Writer
// ---------------------------------------------------------------------------

type resilientWriter struct {
	writer      *kafka.Writer
	retryConfig RetryConfig
	stats       *ProducerStats
}

func newResilientWriter(broker, topic string, batchSize int, retryConfig RetryConfig, stats *ProducerStats) *resilientWriter {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(broker),
		Topic:        topic,
		BatchSize:    batchSize,
		RequiredAcks: kafka.RequireOne,
		Compression:  kafka.Snappy,
		// Add timeouts for better error handling
		WriteTimeout: 10 * time.Second,
		ReadTimeout:  10 * time.Second,
	}

	return &resilientWriter{
		writer:      writer,
		retryConfig: retryConfig,
		stats:       stats,
	}
}

func (rw *resilientWriter) writeMessagesWithRetry(ctx context.Context, msgs []kafka.Message) error {
	var lastErr error

	for attempt := 0; attempt < rw.retryConfig.MaxAttempts; attempt++ {
		if attempt > 0 {
			backoff := calculateBackoff(attempt-1, rw.retryConfig)
			log.Printf("[RETRY] write attempt %d/%d after %v (batch size: %d)",
				attempt+1, rw.retryConfig.MaxAttempts, backoff, len(msgs))
			rw.stats.recordRetry()

			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		err := rw.writer.WriteMessages(ctx, msgs...)
		if err == nil {
			if attempt > 0 {
				log.Printf("[RETRY] write succeeded on attempt %d", attempt+1)
			}
			return nil
		}

		lastErr = err
		rw.stats.recordWriteError()
		log.Printf("[RETRY] write attempt %d failed: %v", attempt+1, err)
	}

	return lastErr
}

func (rw *resilientWriter) close() error {
	if rw.writer == nil {
		return nil
	}

	// Attempt to close gracefully with retry
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Second)
			log.Printf("[RETRY] writer close attempt %d/3", attempt+1)
		}

		err := rw.writer.Close()
		if err == nil {
			if attempt > 0 {
				log.Printf("[RETRY] writer close succeeded on attempt %d", attempt+1)
			}
			return nil
		}

		lastErr = err
		log.Printf("[RETRY] writer close attempt %d failed: %v", attempt+1, err)
	}

	return lastErr
}

// ---------------------------------------------------------------------------
// Worker function with retry logic
// ---------------------------------------------------------------------------

func runWorker(
	ctx context.Context,
	workerID int,
	recordsToGenerate int64,
	writer *resilientWriter,
	globalID *int64,
	stats *ProducerStats,
	batchSize int,
) error {
	r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))
	msgs := make([]kafka.Message, 0, batchSize)

	defer func() {
		// Flush remaining messages on exit
		if len(msgs) > 0 {
			log.Printf("[W%d] flushing %d remaining messages", workerID, len(msgs))
			flushCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			if err := writer.writeMessagesWithRetry(flushCtx, msgs); err != nil {
				log.Printf("[W%d] CRITICAL: failed to flush remaining messages: %v", workerID, err)
				stats.recordWorkerError()
			} else {
				stats.recordSent(int64(len(msgs)))
			}
		}
	}()

	for i := int64(0); i < recordsToGenerate; i++ {
		select {
		case <-ctx.Done():
			log.Printf("[W%d] context cancelled at record %d/%d", workerID, i, recordsToGenerate)
			return ctx.Err()
		default:
		}

		// Generate record
		id := atomic.AddInt64(globalID, 1)
		nameLen := 10 + r.Intn(6)
		addrLen := 15 + r.Intn(6)

		record := make([]byte, 0, 64)
		record = strconv.AppendInt(record, id, 10)
		record = append(record, ',')
		record = append(record, randFromSet(r, letters, nameLen)...)
		record = append(record, ',')
		record = append(record, randFromSet(r, addressChars, addrLen)...)
		record = append(record, ',')
		record = append(record, continents[r.Intn(len(continents))]...)

		msgs = append(msgs, kafka.Message{Value: record})

		// Flush batch when full
		if len(msgs) == cap(msgs) {
			err := writer.writeMessagesWithRetry(ctx, msgs)
			if err != nil {
				log.Printf("[W%d] CRITICAL: write failed after retries: %v", workerID, err)
				stats.recordWorkerError()
				return err
			}

			stats.recordSent(int64(len(msgs)))
			msgs = msgs[:0]
		}

		// Progress logging per worker
		if (i+1)%1000000 == 0 {
			log.Printf("[W%d] progress: %dM/%dM records generated",
				workerID, (i+1)/1000000, recordsToGenerate/1000000)
		}
	}

	return nil
}

// ---------------------------------------------------------------------------
// Main producer function with retry logic
// ---------------------------------------------------------------------------

func Producer() error {
	flag.Parse()

	cfg := ProducerConfig{
		Broker:         *broker,
		Topic:          *topic,
		TotalRecords:   *total,
		Workers:        *workers,
		BatchSize:      *batch,
		RetryConfig:    defaultRetryConfig,
		ShutdownWindow: 30 * time.Second,
	}

	return ProducerWithConfig(cfg)
}

func ProducerWithConfig(cfg ProducerConfig) error {
	start := time.Now()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup signal handling
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		log.Println("shutdown signal received - initiating graceful shutdown")
		cancel()
	}()

	// Initialize statistics
	stats := &ProducerStats{}

	// Create resilient writer
	writer := newResilientWriter(cfg.Broker, cfg.Topic, cfg.BatchSize, cfg.RetryConfig, stats)
	defer func() {
		log.Println("closing writer...")
		if err := writer.close(); err != nil {
			log.Printf("ERROR: writer close failed: %v", err)
		} else {
			log.Println("writer closed successfully")
		}
	}()

	// Start progress monitoring
	monitorCtx, monitorCancel := context.WithCancel(ctx)
	defer monitorCancel()
	go monitorProgress(monitorCtx, stats, start)

	// Launch workers
	var globalID int64
	var wg sync.WaitGroup
	errChan := make(chan error, cfg.Workers)

	perWorker := cfg.TotalRecords / int64(cfg.Workers)
	remainder := cfg.TotalRecords % int64(cfg.Workers)

	log.Printf("starting %d workers to generate %d total records (%d per worker + %d remainder)",
		cfg.Workers, cfg.TotalRecords, perWorker, remainder)

	wg.Add(cfg.Workers)
	for w := 0; w < cfg.Workers; w++ {
		workerRecords := perWorker
		if w == 0 {
			workerRecords += remainder // Give remainder to first worker
		}

		go func(workerID int, records int64) {
			defer wg.Done()

			if err := runWorker(ctx, workerID, records, writer, &globalID, stats, cfg.BatchSize); err != nil {
				if err != context.Canceled {
					log.Printf("[W%d] ERROR: %v", workerID, err)
					errChan <- err
				}
			} else {
				log.Printf("[W%d] completed successfully", workerID)
			}
		}(w, workerRecords)
	}

	// Wait for workers with timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	// Wait for completion or shutdown signal
	select {
	case <-done:
		log.Println("all workers completed")
	case <-ctx.Done():
		log.Println("waiting for workers to finish (graceful shutdown)...")
		
		// Give workers time to finish current batches
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.ShutdownWindow)
		defer shutdownCancel()

		select {
		case <-done:
			log.Println("workers finished gracefully")
		case <-shutdownCtx.Done():
			log.Println("WARNING: shutdown timeout reached, some messages may be lost")
		}
	}

	close(errChan)

	// Check for worker errors
	var workerErrs []error
	for err := range errChan {
		workerErrs = append(workerErrs, err)
	}

	// Final statistics
	sent, writeErrors, retries, workerErrors := stats.snapshot()
	elapsed := time.Since(start)

	log.Printf("========================================")
	log.Printf("Monitor STATISTICS")
	log.Printf("========================================")
	log.Printf("Total sent:       %d records", sent)
	log.Printf("Target:           %d records", cfg.TotalRecords)
	log.Printf("Completion:       %.2f%%", float64(sent)/float64(cfg.TotalRecords)*100)
	log.Printf("Write errors:     %d", writeErrors)
	log.Printf("Retried writes:   %d", retries)
	log.Printf("Worker errors:    %d", workerErrors)
	log.Printf("Elapsed time:     %s", elapsed)
	log.Printf("Throughput:       %.0f records/sec", float64(sent)/elapsed.Seconds())
	log.Printf("========================================")

	if len(workerErrs) > 0 {
		log.Printf("ERROR: %d workers encountered errors", len(workerErrs))
		return workerErrs[0] // Return first error
	}

	if sent < cfg.TotalRecords {
		log.Printf("WARNING: sent %d records but target was %d (%.2f%% complete)",
			sent, cfg.TotalRecords, float64(sent)/float64(cfg.TotalRecords)*100)
	}

	return nil
}

// ---------------------------------------------------------------------------
// Progress monitoring
// ---------------------------------------------------------------------------

func monitorProgress(ctx context.Context, stats *ProducerStats, start time.Time) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	var lastSent int64

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sent, writeErrors, retries, workerErrors := stats.snapshot()
			elapsed := time.Since(start)
			
			currentRate := float64(sent-lastSent) / 10.0 // Records per second over last 10s
			overallRate := float64(sent) / elapsed.Seconds()

			log.Printf("[PROGRESS] sent=%d rate=%.0f/s (current) %.0f/s (avg) errors=(write:%d retry:%d worker:%d)",
				sent, currentRate, overallRate, writeErrors, retries, workerErrors)

			lastSent = sent
		}
	}
}