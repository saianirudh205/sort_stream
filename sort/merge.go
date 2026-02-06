package sort

import (
	"bufio"
	"container/heap"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/IBM/sarama"
)

// Packet represents your data record. 
// Using RawData avoids re-encoding to JSON/Bytes during Phase 2.
type Packet struct {
	ID        int64  `json:"id"`
	Name      string `json:"name"`
	Continent string `json:"continent"`
	RawData   []byte `json:"-"` 
}

// ---------------------------------------------------------------------------
// K-way merge Heap Implementation
// ---------------------------------------------------------------------------

type MergeItem struct {
	packet    Packet
	fileIndex int
}

type MergeHeap struct {
	items   []MergeItem
	sortKey string
}

func (h MergeHeap) Len() int           { return len(h.items) }
func (h MergeHeap) Swap(i, j int)      { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h MergeHeap) Less(i, j int) bool {
	switch h.sortKey {
	case "id":
		return h.items[i].packet.ID < h.items[j].packet.ID
	case "name":
		return h.items[i].packet.Name < h.items[j].packet.Name
	case "continent":
		return h.items[i].packet.Continent < h.items[j].packet.Continent
	}
	return false
}
func (h *MergeHeap) Push(x interface{}) { h.items = append(h.items, x.(MergeItem)) }
func (h *MergeHeap) Pop() interface{} {
	n := len(h.items)
	v := h.items[n-1]
	h.items = h.items[:n-1]
	return v
}

// parsePacket handles the conversion from disk string to Struct
func parsePacket(line string) (Packet, error) {
	var p Packet
	err := json.Unmarshal([]byte(line), &p)
	if err == nil {
		p.RawData = []byte(line)
	}
	return p, err
}

// ---------------------------------------------------------------------------
// Main Merge and Stream Logic
// ---------------------------------------------------------------------------

func MergeAndStream(ctx context.Context, sortKey string, inputFiles []string,
	brokers []string, outputTopic string) error {

	log.Printf("[%s] Starting Merge/Stream: %d files on 4 CPUs", sortKey, len(inputFiles))

	type fileReader struct {
		file    *os.File
		scanner *bufio.Scanner
	}
	
	// Open all files and set large read buffers (1MB per file)
	// With 16 files, this uses ~16MB of RAM, very safe for 2GB total.
	readers := make([]fileReader, 0, len(inputFiles))
	for _, path := range inputFiles {
		f, err := os.Open(path)
		if err != nil {
			if os.IsNotExist(err) { continue }
			return err
		}
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 1024*1024), 2*1024*1024) 
		readers = append(readers, fileReader{file: f, scanner: sc})
	}
	defer func() {
		for _, r := range readers { r.file.Close() }
	}()

	// Kafka Producer Config optimized for 4-CPU Single Machine
	config := sarama.NewConfig()
	config.Producer.Return.Successes = false
	config.Producer.Return.Errors = true
	config.Producer.Compression = sarama.CompressionLZ4 // CPU efficient
	config.Producer.Flush.Messages = 5000               // Batching for throughput
	config.Producer.Flush.Frequency = 100 * time.Millisecond
	config.Producer.RequiredAcks = sarama.WaitForLocal  // Balanced safety/speed

	producer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		return fmt.Errorf("failed to create producer: %v", err)
	}

	// Channel to prevent CPU drop: Heap works while Producer sends.
	// Buffer of 10k items prevents small network blips from stalling the heap.
	msgChan := make(chan *sarama.ProducerMessage, 10000)

	// Worker 1: Producer Input Handler
	go func() {
		for msg := range msgChan {
			producer.Input() <- msg
		}
		producer.Close()
	}()

	// Worker 2: Error Logger
	go func() {
		for err := range producer.Errors() {
			log.Printf("[%s] Kafka Error: %v", sortKey, err)
		}
	}()

	// Initialize the Heap
	mh := &MergeHeap{sortKey: sortKey, items: make([]MergeItem, 0, len(readers))}
	for i, r := range readers {
		if r.scanner.Scan() {
			p, err := parsePacket(r.scanner.Text())
			if err == nil {
				heap.Push(mh, MergeItem{packet: p, fileIndex: i})
			}
		}
	}
	heap.Init(mh)

	count := 0
	start := time.Now()

	// Main Merge Loop
	for mh.Len() > 0 {
		select {
		case <-ctx.Done():
			close(msgChan)
			return ctx.Err()
		default:
		}

		// Get smallest item
		item := heap.Pop(mh).(MergeItem)

		// Stream to Kafka via Channel (This keeps CPU usage up)
		msgChan <- &sarama.ProducerMessage{
			Topic: outputTopic,
			Value: sarama.ByteEncoder(item.packet.RawData),
		}

		count++
		if count%5000000 == 0 {
			log.Printf("[%s] Streamed %d records... Avg speed: %.0f msg/s", 
				sortKey, count, float64(count)/time.Since(start).Seconds())
		}

		// Refill the heap from the file we just popped from
		if readers[item.fileIndex].scanner.Scan() {
			p, err := parsePacket(readers[item.fileIndex].scanner.Text())
			if err == nil {
				heap.Push(mh, MergeItem{packet: p, fileIndex: item.fileIndex})
			}
		}
	}

	close(msgChan)
	elapsed := time.Since(start)
	log.Printf("[%s] FINISHED: %d records in %.2f min", 
		sortKey, count, elapsed.Minutes())
	
	return nil
}