package main

import (
	"bufio"
	"container/heap"
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"
	"strconv"

	"github.com/IBM/sarama"
)

type Packet struct {
	ID        string
	Name      string
	Address   string
	Continent string
	RawData   string
}

func ParsePacket(csvLine string) (Packet, error) {
	reader := csv.NewReader(strings.NewReader(csvLine))
	fields, err := reader.Read()
	if err != nil {
		return Packet{}, err
	}
	if len(fields) < 4 {
		return Packet{}, fmt.Errorf("invalid packet format")
	}
	return Packet{
		ID:        strings.TrimSpace(fields[0]),
		Name:      strings.TrimSpace(fields[1]),
		Address:   strings.TrimSpace(fields[2]),
		Continent: strings.TrimSpace(fields[3]),
		RawData:   csvLine,
	}, nil
}

// ========== HEAP IMPLEMENTATIONS ==========

// ID Heap
type IDHeap []Packet

func (h IDHeap) Len() int           { return len(h) }
func (h IDHeap) Less(i, j int) bool { return h[i].ID < h[j].ID }
func (h IDHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *IDHeap) Push(x interface{}) {
	*h = append(*h, x.(Packet))
}
func (h *IDHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

// Name Heap
type NameHeap []Packet

func (h NameHeap) Len() int           { return len(h) }
func (h NameHeap) Less(i, j int) bool { return h[i].Name < h[j].Name }
func (h NameHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *NameHeap) Push(x interface{}) {
	*h = append(*h, x.(Packet))
}
func (h *NameHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

// Continent Heap
type ContinentHeap []Packet

func (h ContinentHeap) Len() int           { return len(h) }
func (h ContinentHeap) Less(i, j int) bool { return h[i].Continent < h[j].Continent }
func (h ContinentHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *ContinentHeap) Push(x interface{}) {
	*h = append(*h, x.(Packet))
}
func (h *ContinentHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

// ========== HEAP WRITER ==========

type HeapWriter struct {
	sortKey       string
	heapInterface interface{}
	file          *os.File
	writer        *bufio.Writer
	heapSize      int
	flushSize     int
	totalWritten  int
	mu            sync.Mutex
}

func NewHeapWriter(sortKey, filename string, flushSize int) (*HeapWriter, error) {
	file, err := os.Create(filename)
	if err != nil {
		return nil, err
	}

	var heapInterface interface{}
	switch sortKey {
	case "id":
		h := &IDHeap{}
		heap.Init(h)
		heapInterface = h
	case "name":
		h := &NameHeap{}
		heap.Init(h)
		heapInterface = h
	case "continent":
		h := &ContinentHeap{}
		heap.Init(h)
		heapInterface = h
	default:
		return nil, fmt.Errorf("unknown sort key: %s", sortKey)
	}

	return &HeapWriter{
		sortKey:       sortKey,
		heapInterface: heapInterface,
		file:          file,
		writer:        bufio.NewWriterSize(file, 1024*1024), // 1MB buffer
		heapSize:      0,
		flushSize:     flushSize,
		totalWritten:  0,
	}, nil
}

func (hw *HeapWriter) AddPacket(packet Packet) error {
	hw.mu.Lock()
	defer hw.mu.Unlock()

	switch hw.sortKey {
	case "id":
		heap.Push(hw.heapInterface.(*IDHeap), packet)
	case "name":
		heap.Push(hw.heapInterface.(*NameHeap), packet)
	case "continent":
		heap.Push(hw.heapInterface.(*ContinentHeap), packet)
	}

	hw.heapSize++

	// Flush to disk when heap reaches flush size
	if hw.heapSize >= hw.flushSize {
		return hw.flush()
	}

	return nil
}

func (hw *HeapWriter) flush() error {
	// Pop all items from heap and write to file (already sorted)
	for hw.heapSize > 0 {
		var packet Packet
		switch hw.sortKey {
		case "id":
			packet = heap.Pop(hw.heapInterface.(*IDHeap)).(Packet)
		case "name":
			packet = heap.Pop(hw.heapInterface.(*NameHeap)).(Packet)
		case "continent":
			packet = heap.Pop(hw.heapInterface.(*ContinentHeap)).(Packet)
		}

		if _, err := hw.writer.WriteString(packet.RawData + "\n"); err != nil {
			return err
		}
		hw.totalWritten++
		hw.heapSize--
	}

	return hw.writer.Flush()
}

func (hw *HeapWriter) Close() error {
	hw.mu.Lock()
	defer hw.mu.Unlock()

	// Flush remaining items
	if hw.heapSize > 0 {
		if err := hw.flush(); err != nil {
			return err
		}
	}

	hw.writer.Flush()
	return hw.file.Close()
}

func (hw *HeapWriter) GetTotalWritten() int {
	hw.mu.Lock()
	defer hw.mu.Unlock()
	return hw.totalWritten
}

// ========== PARTITION CONSUMER ==========

type PartitionConsumer struct {
	partition     int
	idWriter      *HeapWriter
	nameWriter    *HeapWriter
	continentWriter *HeapWriter
}

func NewPartitionConsumer(partition int, tempDir string, heapFlushSize int) (*PartitionConsumer, error) {
	idWriter, err := NewHeapWriter("id", 
		fmt.Sprintf("%s/id-p%d.txt", tempDir, partition), heapFlushSize)
	if err != nil {
		return nil, err
	}

	nameWriter, err := NewHeapWriter("name", 
		fmt.Sprintf("%s/name-p%d.txt", tempDir, partition), heapFlushSize)
	if err != nil {
		return nil, err
	}

	continentWriter, err := NewHeapWriter("continent", 
		fmt.Sprintf("%s/continent-p%d.txt", tempDir, partition), heapFlushSize)
	if err != nil {
		return nil, err
	}

	return &PartitionConsumer{
		partition:       partition,
		idWriter:        idWriter,
		nameWriter:      nameWriter,
		continentWriter: continentWriter,
	}, nil
}

func (pc *PartitionConsumer) ConsumePartition(ctx context.Context, brokers []string, 
	sourceTopic string, idleTimeout time.Duration) error {
	
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Fetch.Default = 10 * 1024 * 1024

	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		return fmt.Errorf("error creating consumer: %v", err)
	}
	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition(sourceTopic, int32(pc.partition), sarama.OffsetOldest)
	if err != nil {
		return fmt.Errorf("error consuming partition: %v", err)
	}
	defer partitionConsumer.Close()

	count := 0
	lastMsgTime := time.Now()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	log.Printf("[Partition %d] Started consuming...", pc.partition)

	for {
		select {
		case <-ctx.Done():
			log.Printf("[Partition %d] Context cancelled", pc.partition)
			return nil

		case message := <-partitionConsumer.Messages():
			packet, err := ParsePacket(string(message.Value))
			if err != nil {
				log.Printf("[Partition %d] Parse error: %v", pc.partition, err)
				continue
			}

			// Add to all three heaps
			if err := pc.idWriter.AddPacket(packet); err != nil {
				return fmt.Errorf("error adding to id heap: %v", err)
			}
			if err := pc.nameWriter.AddPacket(packet); err != nil {
				return fmt.Errorf("error adding to name heap: %v", err)
			}
			if err := pc.continentWriter.AddPacket(packet); err != nil {
				return fmt.Errorf("error adding to continent heap: %v", err)
			}

			count++
			lastMsgTime = time.Now()

			if count%100000 == 0 {
				log.Printf("[Partition %d] Processed %d messages", pc.partition, count)
			}

		case err := <-partitionConsumer.Errors():
			log.Printf("[Partition %d] Consumer error: %v", pc.partition, err)

		case <-ticker.C:
			// Check idle timeout
			if time.Since(lastMsgTime) > idleTimeout {
				log.Printf("[Partition %d] No messages for %v, stopping (total: %d)", 
					pc.partition, idleTimeout, count)
				return nil
			}
		}
	}
}

func (pc *PartitionConsumer) Close() error {
	log.Printf("[Partition %d] Closing writers...", pc.partition)
	
	if err := pc.idWriter.Close(); err != nil {
		return err
	}
	if err := pc.nameWriter.Close(); err != nil {
		return err
	}
	if err := pc.continentWriter.Close(); err != nil {
		return err
	}

	log.Printf("[Partition %d] Written - ID: %d, Name: %d, Continent: %d",
		pc.partition,
		pc.idWriter.GetTotalWritten(),
		pc.nameWriter.GetTotalWritten(),
		pc.continentWriter.GetTotalWritten())

	return nil
}

// ========== MERGE AND STREAM ==========

type FileMerger struct {
	sortKey    string
	inputFiles []string
}

func NewFileMerger(sortKey string, inputFiles []string) *FileMerger {
	return &FileMerger{
		sortKey:    sortKey,
		inputFiles: inputFiles,
	}
}

type MergeHeapItem struct {
	packet    Packet
	fileIndex int
}

type MergeHeap struct {
	items   []MergeHeapItem
	sortKey string
}

func mustAtoi(s string) int {
    v, _ := strconv.Atoi(s)
    return v
}


func (h MergeHeap) Len() int { return len(h.items) }
func (h MergeHeap) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h MergeHeap) Less(i, j int) bool {
	switch h.sortKey {
	case "id":
		return mustAtoi(h.items[i].packet.ID) < mustAtoi(h.items[j].packet.ID)
	case "name":
		return h.items[i].packet.Name < h.items[j].packet.Name
	case "continent":
		return h.items[i].packet.Continent < h.items[j].packet.Continent
	}
	return false
}
func (h *MergeHeap) Push(x interface{}) {
	h.items = append(h.items, x.(MergeHeapItem))
}
func (h *MergeHeap) Pop() interface{} {
	old := h.items
	n := len(old)
	item := old[n-1]
	h.items = old[0 : n-1]
	return item
}

func (fm *FileMerger) MergeAndStream(ctx context.Context, brokers []string, outputTopic string) error {
	log.Printf("[%s] Starting merge of %d files to %s", fm.sortKey, len(fm.inputFiles), outputTopic)

	// Open all files
	files := make([]*os.File, len(fm.inputFiles))
	scanners := make([]*bufio.Scanner, len(fm.inputFiles))

	for i, filename := range fm.inputFiles {
		file, err := os.Open(filename)
		if err != nil {
			return fmt.Errorf("error opening %s: %v", filename, err)
		}
		files[i] = file
		scanners[i] = bufio.NewScanner(file)
		defer file.Close()
	}

	// Initialize Kafka producer
	config := sarama.NewConfig()
	config.Producer.Return.Successes = false
	config.Producer.Return.Errors = true
	config.Producer.Compression = sarama.CompressionSnappy
	config.Producer.Flush.Messages = 1000
	config.Producer.RequiredAcks = sarama.WaitForLocal
	//config.Producer.Partitioner = sarama.NewManualPartitioner()

	producer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		return fmt.Errorf("error creating producer: %v", err)
	}
	defer producer.Close()

	// Error handler
	go func() {
		for err := range producer.Errors() {
			log.Printf("[%s] Producer error: %v", fm.sortKey, err)
		}
	}()

	// Initialize merge heap
	mergeHeap := &MergeHeap{sortKey: fm.sortKey}
	heap.Init(mergeHeap)

	// Read first line from each file
	for i, scanner := range scanners {
		if scanner.Scan() {
			packet, err := ParsePacket(scanner.Text())
			if err == nil {
				heap.Push(mergeHeap, MergeHeapItem{
					packet:    packet,
					fileIndex: i,
				})
			}
		}
	}

	// K-way merge
	count := 0
	startTime := time.Now()

	for mergeHeap.Len() > 0 {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		item := heap.Pop(mergeHeap).(MergeHeapItem)
		packet := item.packet

		// Send to Kafka (partition 0)
		producer.Input() <- &sarama.ProducerMessage{
			Topic:     outputTopic,
			Value:     sarama.ByteEncoder(packet.RawData),
			Partition: 0,
		}

		count++
		if count%100000 == 0 {
			elapsed := time.Since(startTime).Seconds()
			rate := float64(count) / elapsed
			log.Printf("[%s] Streamed %d packets (%.0f msg/s)", fm.sortKey, count, rate)
		}

		// Read next line from same file
		if scanners[item.fileIndex].Scan() {
			nextPacket, err := ParsePacket(scanners[item.fileIndex].Text())
			if err == nil {
				heap.Push(mergeHeap, MergeHeapItem{
					packet:    nextPacket,
					fileIndex: item.fileIndex,
				})
			}
		}
	}

	producer.Close()

	elapsed := time.Since(startTime)
	log.Printf("[%s] Completed! Streamed %d packets in %.2f seconds (%.0f msg/s)",
		fm.sortKey, count, elapsed.Seconds(), float64(count)/elapsed.Seconds())

	return nil
}

// ========== MAIN ORCHESTRATOR ==========

func CreateOutputTopics(brokers []string, topicNames []string) error {
	config := sarama.NewConfig()
	config.Version = sarama.V2_6_0_0

	admin, err := sarama.NewClusterAdmin(brokers, config)
	if err != nil {
		return err
	}
	defer admin.Close()

	for _, topic := range topicNames {
		topicDetail := &sarama.TopicDetail{
			NumPartitions:     1,
			ReplicationFactor: 1,
		}
		err := admin.CreateTopic(topic, topicDetail, false)
		if err != nil {
			log.Printf("Topic %s might already exist: %v", topic, err)
		} else {
			log.Printf("Created topic: %s", topic)
		}
	}
	return nil
}

func main() {
	brokers := []string{"localhost:9092"}
	sourceTopic := "source"
	numPartitions := 25
	numWorkers := 40   // workers working in parallel
	heapFlushSize := 5000 // Flush heap to disk every 20K packets
	idleTimeout := 2 * time.Minute

	outputTopics := map[string]string{
		"id":        "id",
		"name":      "name",
		"continent": "continent",
	}

	// Create temp directory
	tempDir := "/tmp/kafka-sort"
	os.RemoveAll(tempDir)
	os.MkdirAll(tempDir, 0755)
	defer os.RemoveAll(tempDir)

	fmt.Println("=== Heap-Based Streaming Kafka Sorter ===")
	fmt.Printf("Source: %s (%d partitions)\n", sourceTopic, numPartitions)
	fmt.Printf("Heap flush size: %d packets\n", heapFlushSize)
	fmt.Printf("Idle timeout: %v\n\n", idleTimeout)

	// Create output topics
	topicList := []string{
		outputTopics["id"],
		outputTopics["name"],
		outputTopics["continent"],
	}
	CreateOutputTopics(brokers, topicList)

	ctx := context.Background()
	overallStart := time.Now()

	// ========== PHASE 1: CONSUME AND HEAP-SORT ==========
	fmt.Println("\n=== PHASE 1: Consume and Heap-Sort ===")
	phase1Start := time.Now()

	var wg1 sync.WaitGroup
	consumers := make([]*PartitionConsumer, numPartitions)
	partitionChan := make(chan int, numPartitions)

	// Worker pool
	for w := 0; w < numWorkers; w++ {
		wg1.Add(1)
		go func(workerID int) {
			defer wg1.Done()
			for partition := range partitionChan {
				consumer, err := NewPartitionConsumer(partition, tempDir, heapFlushSize)
				if err != nil {
					log.Printf("Worker %d: Error creating consumer: %v", workerID, err)
					continue
				}
				consumers[partition] = consumer

				if err := consumer.ConsumePartition(ctx, brokers, sourceTopic, idleTimeout); err != nil {
					log.Printf("Worker %d: Error consuming partition %d: %v", workerID, partition, err)
				}

				consumer.Close()
			}
		}(w)
	}

	// Feed partitions
	for p := 0; p < numPartitions; p++ {
		partitionChan <- p
	}
	close(partitionChan)
	wg1.Wait()

	phase1Duration := time.Since(phase1Start)
	fmt.Printf("\nPhase 1 completed in %.2f seconds (%.2f minutes)\n",
		phase1Duration.Seconds(), phase1Duration.Minutes())

	// ========== PHASE 2: MERGE AND STREAM ==========
	fmt.Println("\n=== PHASE 2: Merge and Stream to Output Topics ===")
	phase2Start := time.Now()

	// Collect files for each sort key
	idFiles := []string{}
	nameFiles := []string{}
	continentFiles := []string{}

	for p := 0; p < numPartitions; p++ {
		idFiles = append(idFiles, fmt.Sprintf("%s/id-p%d.txt", tempDir, p))
		nameFiles = append(nameFiles, fmt.Sprintf("%s/name-p%d.txt", tempDir, p))
		continentFiles = append(continentFiles, fmt.Sprintf("%s/continent-p%d.txt", tempDir, p))
	}

	// Merge in parallel
	var wg2 sync.WaitGroup

	// ID sort
	wg2.Add(1)
	go func() {
		defer wg2.Done()
		merger := NewFileMerger("id", idFiles)
		if err := merger.MergeAndStream(ctx, brokers, outputTopics["id"]); err != nil {
			log.Printf("Error merging ID: %v", err)
		}
	}()

	// Name sort
	wg2.Add(1)
	go func() {
		defer wg2.Done()
		merger := NewFileMerger("name", nameFiles)
		if err := merger.MergeAndStream(ctx, brokers, outputTopics["name"]); err != nil {
			log.Printf("Error merging Name: %v", err)
		}
	}()

	// Continent sort
	wg2.Add(1)
	go func() {
		defer wg2.Done()
		merger := NewFileMerger("continent", continentFiles)
		if err := merger.MergeAndStream(ctx, brokers, outputTopics["continent"]); err != nil {
			log.Printf("Error merging Continent: %v", err)
		}
	}()

	wg2.Wait()

	phase2Duration := time.Since(phase2Start)
	fmt.Printf("\nPhase 2 completed in %.2f seconds (%.2f minutes)\n",
		phase2Duration.Seconds(), phase2Duration.Minutes())

	// ========== SUMMARY ==========
	totalDuration := time.Since(overallStart)
	fmt.Println("\n╔════════════════════════════════════════╗")
	fmt.Println("║         PROCESSING COMPLETE            ║")
	fmt.Println("╚════════════════════════════════════════╝")
	fmt.Printf("\nPhase 1 (Consume & Heap):  %.2f min\n", phase1Duration.Minutes())
	fmt.Printf("Phase 2 (Merge & Stream):  %.2f min\n", phase2Duration.Minutes())
	fmt.Printf("Total Time:                %.2f min (%.2f hours)\n",
		totalDuration.Minutes(), totalDuration.Hours())
	fmt.Println("\nOutput Topics:")
	fmt.Printf("  ID sorted:        %s\n", outputTopics["id"])
	fmt.Printf("  Name sorted:      %s\n", outputTopics["name"])
	fmt.Printf("  Continent sorted: %s\n", outputTopics["continent"])

	sigChan := make(chan struct{})
	<-sigChan
}