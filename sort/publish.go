package sort

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

func packAndPush() {
	// ---------------- Configuration ----------------
	brokers := []string{"localhost:9092"}
	sourceTopic := "source"
	numPartitions := 4
	heapFlushSize := 2000
	idleTimeout := 1 * time.Minute

	maxRetries := 3
	retryBackoff := 2 * time.Second

	outputTopics := map[string]string{
		"id":        "id",
		"name":      "name",
		"continent": "continent",
	}

	tempDir := "/tmp/kafka-sort"

	// ---------------- Context / Signals ----------------
	ctx, cancel := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer cancel()

	// ---------------- Temp Dir ----------------
	if tempDir == "" || tempDir == "/" {
		log.Fatalf("unsafe temp dir: %q", tempDir)
	}
	_ = os.RemoveAll(tempDir)
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		log.Fatalf("mkdir %s failed: %v", tempDir, err)
	}
	defer os.RemoveAll(tempDir)

	// ---------------- Startup ----------------
	fmt.Println("=== Heap-Based Streaming Kafka Sorter ===")
	fmt.Printf(
		"Source: %s (%d partitions) | flush=%d | idle=%v | retries=%d\n\n",
		sourceTopic,
		numPartitions,
		heapFlushSize,
		idleTimeout,
		maxRetries,
	)

	createOutputTopics(brokers, []string{
		outputTopics["id"],
		outputTopics["name"],
		outputTopics["continent"],
	})

	overallStart := time.Now()

	// =====================================================================
	// PHASE 1: Consume & Heap Sort (with retries)
	// =====================================================================
	fmt.Println("=== PHASE 1: Consume & Heap-Sort ===")
	phase1Start := time.Now()

	var wg sync.WaitGroup
	errCh := make(chan error, numPartitions)

	wg.Add(numPartitions)
	for p := 0; p < numPartitions; p++ {
		partition := p

		go func() {
			defer wg.Done()

			var attempt int
			for {
				attempt++

				select {
				case <-ctx.Done():
					return
				default:
				}

				_, err := consumePartition(
					ctx,
					partition,
					brokers,
					sourceTopic,
					tempDir,
					heapFlushSize,
					idleTimeout,
				)

				if err == nil {
					return
				}

				if attempt >= maxRetries {
					errCh <- fmt.Errorf(
						"partition %d failed after %d retries: %w",
						partition,
						attempt,
						err,
					)
					return
				}

				log.Printf(
					"partition %d retry %d/%d after error: %v",
					partition,
					attempt,
					maxRetries,
					err,
				)

				select {
				case <-time.After(retryBackoff):
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(errCh)
	}()

	for err := range errCh {
		if err != nil {
			log.Printf("FATAL phase 1 error: %v", err)
			cancel()
			return
		}
	}

	phase1D := time.Since(phase1Start)
	fmt.Printf("Phase 1: %.2fs (%.2f min)\n\n", phase1D.Seconds(), phase1D.Minutes())

	// =====================================================================
	// PHASE 2: Merge & Stream (with retries)
	// =====================================================================
	fmt.Println("=== PHASE 2: Merge & Stream ===")
	phase2Start := time.Now()

	var wg2 sync.WaitGroup
	errCh2 := make(chan error, 3)

	for _, key := range []string{"id", "name", "continent"} {
		sortKey := key

		files := make([]string, numPartitions)
		for p := 0; p < numPartitions; p++ {
			files[p] = fmt.Sprintf("%s/%s-p%d.txt", tempDir, sortKey, p)
		}

		wg2.Add(1)
		go func(k string, f []string) {
			defer wg2.Done()

			var attempt int
			for {
				attempt++

				select {
				case <-ctx.Done():
					return
				default:
				}

				err := mergeAndStream(
					ctx,
					k,
					f,
					brokers,
					outputTopics[k],
				)

				//log.Printf("[DEBUG] mergeAndStream for key=%s returned: err=%v", k, err)


				if err == nil {
					return
				}

				if attempt >= maxRetries {
					errCh2 <- fmt.Errorf(
						"merge %s failed after %d retries: %w",
						k,
						attempt,
						err,
					)
					return
				}

				log.Printf(
					"merge %s retry %d/%d after error: %v",
					k,
					attempt,
					maxRetries,
					err,
				)

				log.Printf("here 221")

				select {
				case <-time.After(retryBackoff):
				case <-ctx.Done():
					return
				}
			}
		}(sortKey, files)
	}

	   
		wg2.Wait()
		close(errCh2)
	

	for err := range errCh2 {
		if err != nil {
			log.Printf("FATAL phase 2 error: %v", err)
			cancel()
			return
		}
	}

	phase2D := time.Since(phase2Start)
	fmt.Printf("Phase 2: %.2fs (%.2f min)\n\n", phase2D.Seconds(), phase2D.Minutes())

	// =====================================================================
	// SUMMARY
	// =====================================================================
	totalD := time.Since(overallStart)

	fmt.Println("╔════════════════════════════════════════╗")
	fmt.Println("║         PROCESSING COMPLETE            ║")
	fmt.Println("╚════════════════════════════════════════╝")
	fmt.Printf("  Phase 1 (Consume & Sort):  %.2f min\n", phase1D.Minutes())
	fmt.Printf("  Phase 2 (Merge & Stream):  %.2f min\n", phase2D.Minutes())
	fmt.Printf("  Total:                     %.2f min\n\n", totalD.Minutes())
	fmt.Printf(
		"  Output: id=%s  name=%s  continent=%s\n",
		outputTopics["id"],
		outputTopics["name"],
		outputTopics["continent"],
	)
}
