package sort

import (
	"bufio"
	"bytes"
	"container/heap"
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/IBM/sarama"
)

// ---------------------------------------------------------------------------
// K-way merge (optimized)
// ---------------------------------------------------------------------------

type MergeItem struct {
	rawLine   []byte
	id        int
	key       []byte
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
		return h.items[i].id < h.items[j].id
	default:
		return bytes.Compare(h.items[i].key, h.items[j].key) < 0
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

// fast CSV key extraction (no allocations, no split)
func extractMergeKey(line []byte, sortKey string) (int, []byte) {
	c1, c2, c3 := -1, -1, -1
	for i := 0; i < len(line); i++ {
		if line[i] == ',' {
			if c1 < 0 {
				c1 = i
			} else if c2 < 0 {
				c2 = i
			} else {
				c3 = i
				break
			}
		}
	}
	if c1 < 0 || c2 < 0 || c3 < 0 {
		return 0, nil
	}

	switch sortKey {
	case "id":
		id := 0
		for i := 0; i < c1; i++ {
			id = id*10 + int(line[i]-'0')
		}
		return id, nil
	case "name":
		return 0, line[c1+1 : c2]
	case "continent":
		return 0, line[c3+1:]
	}
	return 0, nil
}

func mergeAndStream(
	ctx context.Context,
	sortKey string,
	inputFiles []string,
	brokers []string,
	outputTopic string,
) error {

	log.Printf("[%s] merging %d files -> %s", sortKey, len(inputFiles), outputTopic)

	type fileReader struct {
		f  *os.File
		sc *bufio.Scanner
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
		sc.Buffer(make([]byte, 0, 256*1024), 2<<20)
		readers = append(readers, fileReader{f, sc})
	}
	defer func() {
		for _, r := range readers {
			r.f.Close()
		}
	}()

	cfg := sarama.NewConfig()
	cfg.Producer.Return.Errors = true
	cfg.Producer.Compression = sarama.CompressionSnappy
	cfg.Producer.Flush.Messages = 5000
	cfg.Producer.Flush.Bytes = 10 * 1024 * 1024
	cfg.Producer.RequiredAcks = sarama.NoResponse
	cfg.ChannelBufferSize = 10000

	producer, err := sarama.NewAsyncProducer(brokers, cfg)
	

	if err != nil {
		return fmt.Errorf("producer create: %v", err)
	}
	 defer producer.AsyncClose()


	go func() {
		log.Printf("Here 141")
		for e := range producer.Errors() {
			log.Printf("[%s] producer error: %v", sortKey, e)
		}
	}()

	h := &MergeHeap{sortKey: sortKey, items: make([]MergeItem, 0, len(readers))}
	heap.Init(h)

	// prime heap
	for i, r := range readers {
		if r.sc.Scan() {
			line := append([]byte(nil), r.sc.Bytes()...)
			id, key := extractMergeKey(line, sortKey)
			if key != nil || sortKey == "id" {
				heap.Push(h, MergeItem{
					rawLine:   line,
					id:        id,
					key:       key,
					fileIndex: i,
				})
			}
		}
	}

	start := time.Now()
	count := 0
	lastLog := time.Now()

	batch := make([]*sarama.ProducerMessage, 0, 5000)

	for h.Len() > 0 {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		it := heap.Pop(h).(MergeItem)

		batch = append(batch, &sarama.ProducerMessage{
			Topic: outputTopic,
			Value: sarama.ByteEncoder(it.rawLine),
		})

		if len(batch) == cap(batch) {
			for _, m := range batch {
				producer.Input() <- m
			}
			batch = batch[:0]
		}

		if readers[it.fileIndex].sc.Scan() {
			line := append([]byte(nil), readers[it.fileIndex].sc.Bytes()...)
			id, key := extractMergeKey(line, sortKey)
			if key != nil || sortKey == "id" {
				heap.Push(h, MergeItem{
					rawLine:   line,
					id:        id,
					key:       key,
					fileIndex: it.fileIndex,
				})
			}
		}

		count++
		if time.Since(lastLog) > 20*time.Second {
			log.Printf("[%s] streamed %d (%.0f msg/s)",
				sortKey,
				count,
				float64(count)/time.Since(start).Seconds(),
			)
			lastLog = time.Now()
		}
	}

	for _, m := range batch {
		producer.Input() <- m
	}

	elapsed := time.Since(start)
	log.Printf("[%s] done – %d packets in %.2fs (%.0f msg/s)",
		sortKey,
		count,
		elapsed.Seconds(),
		float64(count)/elapsed.Seconds(),
	)
	
	//cancel()
	return nil
}
