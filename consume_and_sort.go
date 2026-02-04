package main

import (
	"bufio"
	"container/heap"
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

// ---------------------------------------------------------------------------
// Packet
// ---------------------------------------------------------------------------

type Packet struct {
	RawData   string
	ID        int
	Name      string
	Continent string
}

func parsePacket(line string) (Packet, error) {
	f1 := strings.IndexByte(line, ',')
	if f1 < 0 {
		return Packet{}, fmt.Errorf("bad line: missing field 1")
	}
	f2 := strings.IndexByte(line[f1+1:], ',')
	if f2 < 0 {
		return Packet{}, fmt.Errorf("bad line: missing field 2")
	}
	f2 += f1 + 1
	f3 := strings.IndexByte(line[f2+1:], ',')
	if f3 < 0 {
		return Packet{}, fmt.Errorf("bad line: missing field 3")
	}
	f3 += f2 + 1

	id, err := strconv.Atoi(line[:f1])
	if err != nil {
		return Packet{}, fmt.Errorf("bad id %q: %w", line[:f1], err)
	}

	return Packet{
		RawData:   line,
		ID:        id,
		Name:      line[f1+1 : f2],
		Continent: line[f3+1:],
	}, nil
}

// ---------------------------------------------------------------------------
// Index-based heaps – all three share a *[]Packet (pointer-to-slice).
// This is the critical part: every heap dereferences the SAME pointer,
// so when AddPacket appends or flush resets the slice, all heaps see it
// instantly. No stale references, no leaked memory.
// ---------------------------------------------------------------------------

type IDHeap struct {
	idx     []int32
	packets *[]Packet // pointer-to-slice, shared
}

func (h IDHeap) Len() int            { return len(h.idx) }
func (h IDHeap) Less(i, j int) bool  { return (*h.packets)[h.idx[i]].ID < (*h.packets)[h.idx[j]].ID }
func (h IDHeap) Swap(i, j int)       { h.idx[i], h.idx[j] = h.idx[j], h.idx[i] }
func (h *IDHeap) Push(x interface{}) { h.idx = append(h.idx, x.(int32)) }
func (h *IDHeap) Pop() interface{} {
	n := len(h.idx)
	v := h.idx[n-1]
	h.idx = h.idx[:n-1]
	return v
}

type NameHeap struct {
	idx     []int32
	packets *[]Packet
}

func (h NameHeap) Len() int            { return len(h.idx) }
func (h NameHeap) Less(i, j int) bool  { return (*h.packets)[h.idx[i]].Name < (*h.packets)[h.idx[j]].Name }
func (h NameHeap) Swap(i, j int)       { h.idx[i], h.idx[j] = h.idx[j], h.idx[i] }
func (h *NameHeap) Push(x interface{}) { h.idx = append(h.idx, x.(int32)) }
func (h *NameHeap) Pop() interface{} {
	n := len(h.idx)
	v := h.idx[n-1]
	h.idx = h.idx[:n-1]
	return v
}

type ContinentHeap struct {
	idx     []int32
	packets *[]Packet
}

func (h ContinentHeap) Len() int { return len(h.idx) }
func (h ContinentHeap) Less(i, j int) bool {
	return (*h.packets)[h.idx[i]].Continent < (*h.packets)[h.idx[j]].Continent
}
func (h ContinentHeap) Swap(i, j int)       { h.idx[i], h.idx[j] = h.idx[j], h.idx[i] }
func (h *ContinentHeap) Push(x interface{}) { h.idx = append(h.idx, x.(int32)) }
func (h *ContinentHeap) Pop() interface{} {
	n := len(h.idx)
	v := h.idx[n-1]
	h.idx = h.idx[:n-1]
	return v
}

// ---------------------------------------------------------------------------
// HeapWriter
// ---------------------------------------------------------------------------

type HeapWriter struct {
	packets *[]Packet // pointer-to-slice – the single source of truth

	idHeap        *IDHeap
	nameHeap      *NameHeap
	continentHeap *ContinentHeap

	idFile          *os.File
	nameFile        *os.File
	continentFile   *os.File
	idWriter        *bufio.Writer
	nameWriter      *bufio.Writer
	continentWriter *bufio.Writer

	flushSize    int
	totalWritten int
	mu           sync.Mutex
}

func NewHeapWriter(partition int, tempDir string, flushSize int) (*HeapWriter, error) {
	openFile := func(name string) (*os.File, error) {
		return os.Create(fmt.Sprintf("%s/%s-p%d.txt", tempDir, name, partition))
	}

	idFile, err := openFile("id")
	if err != nil {
		return nil, err
	}
	nameFile, err := openFile("name")
	if err != nil {
		idFile.Close()
		return nil, err
	}
	continentFile, err := openFile("continent")
	if err != nil {
		idFile.Close()
		nameFile.Close()
		return nil, err
	}

	packets := make([]Packet, 0, flushSize)
	pPtr := &packets // one pointer, shared by everything

	return &HeapWriter{
		packets:       pPtr,
		idHeap:        &IDHeap{idx: make([]int32, 0, flushSize), packets: pPtr},
		nameHeap:      &NameHeap{idx: make([]int32, 0, flushSize), packets: pPtr},
		continentHeap: &ContinentHeap{idx: make([]int32, 0, flushSize), packets: pPtr},

		idFile:          idFile,
		nameFile:        nameFile,
		continentFile:   continentFile,
		idWriter:        bufio.NewWriterSize(idFile, 1<<20),
		nameWriter:      bufio.NewWriterSize(nameFile, 1<<20),
		continentWriter: bufio.NewWriterSize(continentFile, 1<<20),

		flushSize: flushSize,
	}, nil
}

func (hw *HeapWriter) AddPacket(p Packet) error {
	hw.mu.Lock()
	defer hw.mu.Unlock()

	idx := int32(len(*hw.packets))
	*hw.packets = append(*hw.packets, p)
	// No re-pointing needed. Heaps hold pPtr, they dereference it every time.

	heap.Push(hw.idHeap, idx)
	heap.Push(hw.nameHeap, idx)
	heap.Push(hw.continentHeap, idx)

	if len(*hw.packets) >= hw.flushSize {
		return hw.flush()
	}
	return nil
}

func (hw *HeapWriter) flush() error {
	pkts := *hw.packets

	for hw.idHeap.Len() > 0 {
		idx := heap.Pop(hw.idHeap).(int32)
		hw.idWriter.WriteString(pkts[idx].RawData)
		hw.idWriter.WriteByte('\n')
	}
	if err := hw.idWriter.Flush(); err != nil {
		return err
	}

	for hw.nameHeap.Len() > 0 {
		idx := heap.Pop(hw.nameHeap).(int32)
		hw.nameWriter.WriteString(pkts[idx].RawData)
		hw.nameWriter.WriteByte('\n')
	}
	if err := hw.nameWriter.Flush(); err != nil {
		return err
	}

	for hw.continentHeap.Len() > 0 {
		idx := heap.Pop(hw.continentHeap).(int32)
		hw.continentWriter.WriteString(pkts[idx].RawData)
		hw.continentWriter.WriteByte('\n')
	}
	if err := hw.continentWriter.Flush(); err != nil {
		return err
	}

	hw.totalWritten += len(pkts)

	// Reset. The backing array stays allocated for reuse – only length goes to 0.
	*hw.packets = (*hw.packets)[:0]
	hw.idHeap.idx = hw.idHeap.idx[:0]
	hw.nameHeap.idx = hw.nameHeap.idx[:0]
	hw.continentHeap.idx = hw.continentHeap.idx[:0]

	return nil
}

func (hw *HeapWriter) Close() error {
	hw.mu.Lock()
	defer hw.mu.Unlock()

	if len(*hw.packets) > 0 {
		if err := hw.flush(); err != nil {
			return err
		}
	}

	hw.idWriter.Flush()
	hw.nameWriter.Flush()
	hw.continentWriter.Flush()

	hw.idFile.Close()
	hw.nameFile.Close()
	hw.continentFile.Close()
	return nil
}

func (hw *HeapWriter) GetTotalWritten() int {
	hw.mu.Lock()
	defer hw.mu.Unlock()
	return hw.totalWritten
}

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
			if count%100000 == 0 {
				log.Printf("[P%d] %d messages", partition, count)
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

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

func main() {
	brokers := []string{"localhost:9092"}
	sourceTopic := "source"
	numPartitions := 20
	heapFlushSize := 5000
	idleTimeout := 2 * time.Minute

	outputTopics := map[string]string{
		"id":        "id",
		"name":      "name",
		"continent": "continent",
	}

	tempDir := "/tmp/kafka-sort"
	os.RemoveAll(tempDir)
	os.MkdirAll(tempDir, 0755)
	defer os.RemoveAll(tempDir)

	fmt.Println("=== Heap-Based Streaming Kafka Sorter ===")
	fmt.Printf("Source: %s (%d partitions) | flush=%d | idle=%v\n\n",
		sourceTopic, numPartitions, heapFlushSize, idleTimeout)

	createOutputTopics(brokers, []string{
		outputTopics["id"], outputTopics["name"], outputTopics["continent"],
	})

	ctx := context.Background()
	overallStart := time.Now()

	// ====== PHASE 1 ======
	fmt.Println("=== PHASE 1: Consume & Heap-Sort ===")
	phase1Start := time.Now()

	var wg sync.WaitGroup
	wg.Add(numPartitions)
	for p := 0; p < numPartitions; p++ {
		go func(partition int) {
			defer wg.Done()
			if _, err := consumePartition(ctx, partition, brokers, sourceTopic,
				tempDir, heapFlushSize, idleTimeout); err != nil {
				log.Printf("partition %d error: %v", partition, err)
			}
		}(p)
	}
	wg.Wait()

	phase1D := time.Since(phase1Start)
	fmt.Printf("Phase 1: %.2fs (%.2f min)\n\n", phase1D.Seconds(), phase1D.Minutes())

	// ====== PHASE 2 ======
	fmt.Println("=== PHASE 2: Merge & Stream ===")
	phase2Start := time.Now()

	var wg2 sync.WaitGroup
	for _, key := range []string{"id", "name", "continent"} {
		files := make([]string, numPartitions)
		for p := 0; p < numPartitions; p++ {
			files[p] = fmt.Sprintf("%s/%s-p%d.txt", tempDir, key, p)
		}
		wg2.Add(1)
		go func(k string, f []string) {
			defer wg2.Done()
			if err := mergeAndStream(ctx, k, f, brokers, outputTopics[k]); err != nil {
				log.Printf("merge %s error: %v", k, err)
			}
		}(key, files)
	}
	wg2.Wait()

	phase2D := time.Since(phase2Start)
	fmt.Printf("Phase 2: %.2fs (%.2f min)\n\n", phase2D.Seconds(), phase2D.Minutes())

	// ====== Summary ======
	totalD := time.Since(overallStart)
	fmt.Println("╔════════════════════════════════════════╗")
	fmt.Println("║         PROCESSING COMPLETE            ║")
	fmt.Println("╚════════════════════════════════════════╝")
	fmt.Printf("  Phase 1 (Consume & Sort):  %.2f min\n", phase1D.Minutes())
	fmt.Printf("  Phase 2 (Merge & Stream):  %.2f min\n", phase2D.Minutes())
	fmt.Printf("  Total:                     %.2f min\n\n", totalD.Minutes())
	fmt.Printf("  Output: id=%s  name=%s  continent=%s\n",
		outputTopics["id"], outputTopics["name"], outputTopics["continent"])


	sigChan := make(chan struct{})
	<-sigChan
}