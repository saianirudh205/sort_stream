package sort

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

func packAndPush() {
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


	test()
	sigChan := make(chan struct{})
	<-sigChan
}
