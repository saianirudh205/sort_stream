package sort

import (
	"bufio"
	"container/heap"
	"context"
	"fmt"
	"log"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
)

// ---------------------------------------------------------------------------
// Retry Configuration
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

// calculateBackoff returns exponential backoff duration with jitter
func calculateBackoff(attempt int, config RetryConfig) time.Duration {
	backoff := float64(config.InitialBackoff) * math.Pow(config.BackoffMultiple, float64(attempt))
	if backoff > float64(config.MaxBackoff) {
		backoff = float64(config.MaxBackoff)
	}
	// Add jitter: 80-100% of calculated backoff
	jitter := 0.8 + (0.2 * float64(time.Now().UnixNano()%100) / 100.0)
	return time.Duration(backoff * jitter)
}

// retryWithBackoff executes a function with exponential backoff retry logic
func retryWithBackoff(ctx context.Context, config RetryConfig, operation string, fn func() error) error {
	var lastErr error
	
	for attempt := 0; attempt < config.MaxAttempts; attempt++ {
		if attempt > 0 {
			backoff := calculateBackoff(attempt-1, config)
			log.Printf("[RETRY] %s: attempt %d/%d after %v", operation, attempt+1, config.MaxAttempts, backoff)
			
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		
		err := fn()
		if err == nil {
			if attempt > 0 {
				log.Printf("[RETRY] %s: succeeded on attempt %d", operation, attempt+1)
			}
			return nil
		}
		
		lastErr = err
		log.Printf("[RETRY] %s: attempt %d failed: %v", operation, attempt+1, err)
	}
	
	return fmt.Errorf("%s failed after %d attempts: %w", operation, config.MaxAttempts, lastErr)
}

// ---------------------------------------------------------------------------
// K-way merge (production-ready, at-least-once)
// ---------------------------------------------------------------------------

type MergeItem struct {
	packet    Packet
	fileIndex int
}

type MergeHeap struct {
	items   []MergeItem
	sortKey string
}

func (h MergeHeap) Len() int      { return len(h.items) }
func (h MergeHeap) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }

func (h MergeHeap) Less(i, j int) bool {
	switch h.sortKey {
	case "id":
		return h.items[i].packet.ID < h.items[j].packet.ID
	case "name":
		return h.items[i].packet.Name < h.items[j].packet.Name
	case "continent":
		return h.items[i].packet.Continent < h.items[j].packet.Continent
	default:
		return false
	}
}

func (h *MergeHeap) Push(x interface{}) {
	h.items = append(h.items, x.(MergeItem))
}

func (h *MergeHeap) Pop() interface{} {
	n := len(h.items)
	v := h.items[n-1]
	h.items = h.items[:n-1]
	return v
}

// ---------------------------------------------------------------------------
// Producer with retry logic
// ---------------------------------------------------------------------------

type resilientProducer struct {
	producer    sarama.AsyncProducer
	brokers     []string
	config      *sarama.Config
	retryConfig RetryConfig
	mu          sync.Mutex
}

func newResilientProducer(brokers []string, cfg *sarama.Config, retryConfig RetryConfig) (*resilientProducer, error) {
	rp := &resilientProducer{
		brokers:     brokers,
		config:      cfg,
		retryConfig: retryConfig,
	}
	
	// Create initial producer with retry
	err := retryWithBackoff(context.Background(), retryConfig, "create producer", func() error {
		producer, err := sarama.NewAsyncProducer(brokers, cfg)
		if err != nil {
			return err
		}
		rp.producer = producer
		return nil
	})
	
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}
	
	return rp, nil
}

func (rp *resilientProducer) sendWithRetry(ctx context.Context, msg *sarama.ProducerMessage) error {
	return retryWithBackoff(ctx, rp.retryConfig, "send message", func() error {
		select {
		case rp.producer.Input() <- msg:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
			return fmt.Errorf("timeout sending message to producer input channel")
		}
	})
}

func (rp *resilientProducer) Input() chan<- *sarama.ProducerMessage {
	return rp.producer.Input()
}

func (rp *resilientProducer) Successes() <-chan *sarama.ProducerMessage {
	return rp.producer.Successes()
}

func (rp *resilientProducer) Errors() <-chan *sarama.ProducerError {
	return rp.producer.Errors()
}

func (rp *resilientProducer) AsyncClose() {
	rp.producer.AsyncClose()
}

func (rp *resilientProducer) Close() error {
	return rp.producer.Close()
}

// ---------------------------------------------------------------------------
// File reader with retry logic
// ---------------------------------------------------------------------------

type resilientFileReader struct {
	path        string
	file        *os.File
	scanner     *bufio.Scanner
	retryConfig RetryConfig
}

func newResilientFileReader(path string, retryConfig RetryConfig) (*resilientFileReader, error) {
	rfr := &resilientFileReader{
		path:        path,
		retryConfig: retryConfig,
	}
	
	err := retryWithBackoff(context.Background(), retryConfig, fmt.Sprintf("open file %s", path), func() error {
		f, err := os.Open(path)
		if err != nil {
			if os.IsNotExist(err) {
				return fmt.Errorf("file does not exist: %w", err)
			}
			return err
		}
		rfr.file = f
		rfr.scanner = bufio.NewScanner(f)
		rfr.scanner.Buffer(make([]byte, 0, 256*1024), 1<<20)
		return nil
	})
	
	if err != nil {
		return nil, err
	}
	
	return rfr, nil
}

func (rfr *resilientFileReader) scanWithRetry(ctx context.Context) (bool, error) {
	var scanned bool
	err := retryWithBackoff(ctx, rfr.retryConfig, "scan file line", func() error {
		scanned = rfr.scanner.Scan()
		if !scanned && rfr.scanner.Err() != nil {
			return rfr.scanner.Err()
		}
		return nil
	})
	
	return scanned, err
}

func (rfr *resilientFileReader) Text() string {
	return rfr.scanner.Text()
}

func (rfr *resilientFileReader) Err() error {
	return rfr.scanner.Err()
}

func (rfr *resilientFileReader) Close() error {
	if rfr.file != nil {
		return rfr.file.Close()
	}
	return nil
}

// ---------------------------------------------------------------------------
// Enhanced merge and stream with retry logic
// ---------------------------------------------------------------------------

func mergeAndStream(
	ctx context.Context,
	sortKey string,
	inputFiles []string,
	brokers []string,
	outputTopic string,
) error {
	return mergeAndStreamWithRetry(ctx, sortKey, inputFiles, brokers, outputTopic, defaultRetryConfig)
}

func mergeAndStreamWithRetry(
	ctx context.Context,
	sortKey string,
	inputFiles []string,
	brokers []string,
	outputTopic string,
	retryConfig RetryConfig,
) error {

	log.Printf("[%s] merging %d files -> %s", sortKey, len(inputFiles), outputTopic)

	// ---------------- File readers with retry ----------------

	readers := make([]*resilientFileReader, 0, len(inputFiles))
	for _, path := range inputFiles {
		rfr, err := newResilientFileReader(path, retryConfig)
		if err != nil {
			if os.IsNotExist(err) {
				log.Printf("[%s] skipping non-existent file: %s", sortKey, path)
				continue
			}
			// Close already opened readers
			for _, r := range readers {
				_ = r.Close()
			}
			return fmt.Errorf("failed to open file %s: %w", path, err)
		}
		readers = append(readers, rfr)
	}

	defer func() {
		for _, r := range readers {
			_ = r.Close()
		}
	}()

	if len(readers) == 0 {
		return fmt.Errorf("no valid input files to process")
	}

	// ---------------- Kafka producer with retry ----------------

	cfg := sarama.NewConfig()
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true
	cfg.Producer.Compression = sarama.CompressionSnappy
	cfg.Producer.Flush.Messages = 2000
	cfg.Producer.Flush.Frequency = 50 * time.Millisecond
	cfg.ChannelBufferSize = 10000
	cfg.Producer.Retry.Max = retryConfig.MaxAttempts
	cfg.Producer.Retry.Backoff = retryConfig.InitialBackoff

	producer, err := newResilientProducer(brokers, cfg, retryConfig)
	if err != nil {
		return err
	}

	var delivered int64
	var producerErrors int64
	var successWG sync.WaitGroup
	var errorWG sync.WaitGroup
	
	successWG.Add(1)
	errorWG.Add(1)

	// Success handler
	go func() {
		defer successWG.Done()
		for range producer.Successes() {
			atomic.AddInt64(&delivered, 1)
		}
	}()

	// Error handler with retry logic
	go func() {
		defer errorWG.Done()
		for e := range producer.Errors() {
			atomic.AddInt64(&producerErrors, 1)
			log.Printf("[%s] producer error (will retry): topic=%s partition=%d offset=%d error=%v",
				sortKey, e.Msg.Topic, e.Msg.Partition, e.Msg.Offset, e.Err)
			
			// Retry failed message
			retryErr := retryWithBackoff(ctx, retryConfig, "resend failed message", func() error {
				select {
				case producer.Input() <- e.Msg:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(5 * time.Second):
					return fmt.Errorf("timeout resending message")
				}
			})
			
			if retryErr != nil {
				log.Printf("[%s] CRITICAL: failed to retry message after %d attempts: %v",
					sortKey, retryConfig.MaxAttempts, retryErr)
			}
		}
	}()

	// ---------------- Heap init with retry ----------------

	mh := &MergeHeap{sortKey: sortKey, items: make([]MergeItem, 0, len(readers))}
	heap.Init(mh)

	for i, r := range readers {
		scanned, err := r.scanWithRetry(ctx)
		if err != nil {
			log.Printf("[%s] error scanning first line file %d: %v", sortKey, i, err)
			continue
		}
		
		if scanned {
			var p Packet
			parseErr := retryWithBackoff(ctx, retryConfig, fmt.Sprintf("parse first packet file %d", i), func() error {
				var err error
				p, err = parsePacket(r.Text())
				return err
			})
			
			if parseErr != nil {
				log.Printf("[%s] skip bad first line file %d after retries: %v", sortKey, i, parseErr)
				continue
			}
			
			heap.Push(mh, MergeItem{packet: p, fileIndex: i})
		}
	}

	start := time.Now()
	var produced int64

	// ---------------- Merge loop with retry ----------------

	for mh.Len() > 0 {
		select {
		case <-ctx.Done():
			producer.AsyncClose()
			successWG.Wait()
			errorWG.Wait()
			return ctx.Err()
		default:
		}

		item := heap.Pop(mh).(MergeItem)

		msg := &sarama.ProducerMessage{
			Topic: outputTopic,
			Value: sarama.ByteEncoder(item.packet.RawData),
		}

		// Send with retry
		err := producer.sendWithRetry(ctx, msg)
		if err != nil {
			log.Printf("[%s] CRITICAL: failed to send message after retries: %v", sortKey, err)
			producer.AsyncClose()
			successWG.Wait()
			errorWG.Wait()
			return fmt.Errorf("failed to send message: %w", err)
		}
		
		atomic.AddInt64(&produced, 1)

		// Read next line with retry
		scanned, err := readers[item.fileIndex].scanWithRetry(ctx)
		if err != nil {
			log.Printf("[%s] error scanning file %d: %v", sortKey, item.fileIndex, err)
			continue
		}
		
		if scanned {
			var p Packet
			parseErr := retryWithBackoff(ctx, retryConfig, fmt.Sprintf("parse packet file %d", item.fileIndex), func() error {
				var err error
				p, err = parsePacket(readers[item.fileIndex].Text())
				return err
			})
			
			if parseErr != nil {
				log.Printf("[%s] skip bad line file %d after retries: %v", sortKey, item.fileIndex, parseErr)
				continue
			}
			
			heap.Push(mh, MergeItem{packet: p, fileIndex: item.fileIndex})
		}

		if produced%10000000 == 0 {
			log.Printf("[%s] streamed %d (%.0f msg/s, %d errors)",
				sortKey,
				produced,
				float64(produced)/time.Since(start).Seconds(),
				atomic.LoadInt64(&producerErrors),
			)
		}
	}

	// ---------------- Finalize ----------------

	producer.AsyncClose()
	successWG.Wait()
	errorWG.Wait()

	finalDelivered := atomic.LoadInt64(&delivered)
	finalErrors := atomic.LoadInt64(&producerErrors)

	if finalDelivered != produced {
		return fmt.Errorf(
			"[%s] delivery mismatch: produced=%d delivered=%d errors=%d",
			sortKey,
			produced,
			finalDelivered,
			finalErrors,
		)
	}

	elapsed := time.Since(start)
	log.Printf("[%s] DONE – %d packets in %.2fs (%.0f msg/s, %d errors recovered)",
		sortKey,
		produced,
		elapsed.Seconds(),
		float64(produced)/elapsed.Seconds(),
		finalErrors,
	)

	return nil
}