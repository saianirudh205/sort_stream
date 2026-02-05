package sort

import (
	"bufio"
	"container/heap"
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/IBM/sarama"
)

// ---------------------------------------------------------------------------
// K-way merge
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

func mergeAndStream(ctx context.Context, sortKey string, inputFiles []string,
	brokers []string, outputTopic string) error {

	log.Printf("[%s] merging %d files -> %s", sortKey, len(inputFiles), outputTopic)

	type fileReader struct {
		file    *os.File
		scanner *bufio.Scanner
	}
	readers := make([]fileReader, 0, len(inputFiles))
	for _, path := range inputFiles {
		f, err := os.Open(path)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return err
		}
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 0, 256*1024), 1<<20)
		readers = append(readers, fileReader{file: f, scanner: sc})
	}
	defer func() {
		for _, r := range readers {
			r.file.Close()
		}
	}()

	config := sarama.NewConfig()
	config.Producer.Return.Successes = false
	config.Producer.Return.Errors = true
	config.Producer.Compression = sarama.CompressionSnappy
	config.Producer.Flush.Messages = 2000
	config.Producer.Flush.Frequency = 50 * time.Millisecond
	config.Producer.RequiredAcks = sarama.NoResponse

	producer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		return fmt.Errorf("producer create: %v", err)
	}

	go func() {
		for err := range producer.Errors() {
			log.Printf("[%s] producer error: %v", sortKey, err)
		}
	}()

	mh := &MergeHeap{sortKey: sortKey, items: make([]MergeItem, 0, len(readers))}
	heap.Init(mh)

	for i, r := range readers {
		if r.scanner.Scan() {
			p, err := parsePacket(r.scanner.Text())
			if err != nil {
				log.Printf("[%s] skip bad first line file %d: %v", sortKey, i, err)
				continue
			}
			heap.Push(mh, MergeItem{packet: p, fileIndex: i})
		}
	}

	count := 0
	start := time.Now()

	for mh.Len() > 0 {
		select {
		case <-ctx.Done():
			producer.Close()
			return nil
		default:
		}

		item := heap.Pop(mh).(MergeItem)

		producer.Input() <- &sarama.ProducerMessage{
			Topic: outputTopic,
			Value: sarama.ByteEncoder(item.packet.RawData),
		}

		count++
		if count%100000 == 0 {
			log.Printf("[%s] streamed %d (%.0f msg/s)", sortKey, count,
				float64(count)/time.Since(start).Seconds())
		}

		if readers[item.fileIndex].scanner.Scan() {
			p, err := parsePacket(readers[item.fileIndex].scanner.Text())
			if err != nil {
				log.Printf("[%s] skip bad line file %d: %v", sortKey, item.fileIndex, err)
				continue
			}
			heap.Push(mh, MergeItem{packet: p, fileIndex: item.fileIndex})
		}
	}

	producer.Close()

	elapsed := time.Since(start)
	log.Printf("[%s] done – %d packets in %.2fs (%.0f msg/s)",
		sortKey, count, elapsed.Seconds(), float64(count)/elapsed.Seconds())
	return nil
}
