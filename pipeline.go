package main

import (

	"context"

	"fmt"
	"math/rand"

	

	"time"
	"sync"
	"sync/atomic"

//	"compress/gzip" 

	"github.com/segmentio/kafka-go"
	
)

const (
	broker      = "localhost:9092"
	sourceTopic = "source"
	dataDir     = "/data"
	chunkSize   = 200_000 // ~ few MB per chunk
	totalRows   = 50_000_000
)

/*
========================
DATA MODEL
========================
Example:
276696866,name b6pllSJFD0,address 1k1wTlbl9uy4AW7ibdWL,Continent Australia
*/
type Record struct {
	ID        int
	Name      string
	Address   string
	Continent string
	Raw       string
}




func generateAndSendParallel() {
	fmt.Println("🚀 PARALLEL Direct Generation")
	
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      []string{"localhost:9092"},
		Topic:        "source",
		BatchSize:    50000,
		Async:        true,
	})
	defer writer.Close()
	
	total := 5000000
	numWorkers := 25  // using 30 workers to run tasks in parallel to add data to the kafka topic 
	messagesPerWorker := total / numWorkers


	
	var wg sync.WaitGroup
	wg.Add(numWorkers)
	
	start := time.Now()
	var totalSent int64
	
	for w := 0; w < numWorkers; w++ {
		go func(workerID int) {
			defer wg.Done()
			
			localRand := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))
			continents := [6]string{"North America", "Asia", "South America", "Europe", "Africa", "Australia"}

			
			for i := 0; i < messagesPerWorker; i++ {
				id := localRand.Int31n(5_100_000)
				name := randomStringLocal(localRand, 10)
				addr := randomStringLocal(localRand, 20)
				continent := continents[localRand.Intn(6)]
				
				msg := fmt.Sprintf("%d,%s,%s,%s", id, name, addr, continent)
				
				writer.WriteMessages(context.Background(),
					kafka.Message{Value: []byte(msg)},
				)
				
				// Progress
				if atomic.AddInt64(&totalSent, 1)%100000 == 0 {
					elapsed := time.Since(start).Seconds()
					rate := float64(atomic.LoadInt64(&totalSent)) / elapsed
					if workerID == 0 {
						fmt.Printf("Sent: %d (%.0f msg/sec)\n", 
							atomic.LoadInt64(&totalSent), rate)
					}
				}
			}
		}(w)
	}
	
	wg.Wait()
	
	// Wait for async writes
	time.Sleep(5 * time.Second)
	
	fmt.Printf("✅ Sent %d messages in %v\n", total, time.Since(start))
}

func randomStringLocal(r *rand.Rand, n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz "
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[r.Intn(len(letters))]
	}
	return string(b)
}


func main() {
	fmt.Println("Generating source data...")

	
	generateAndSendParallel()
	

}
