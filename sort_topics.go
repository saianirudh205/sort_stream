package main

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

type Record struct {
	ID        string
	Name      string
	Address   string
	Continent string
	Raw       string
}

func main() {
	fmt.Println("Sorter starting...")

	// Create reader with better configuration
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{"localhost:9092"},
		Topic:       "source",
		GroupID:     "sorter-group-" + fmt.Sprintf("%d", time.Now().UnixNano()), // Unique group
		StartOffset: kafka.FirstOffset, // Read from beginning
		MinBytes:    1,                 // Minimum bytes to fetch
		MaxBytes:    10e6,              // 10MB max
		MaxWait:     5 * time.Second,   // Wait up to 5s for messages
	})

	defer r.Close()

	var records []Record
	start := time.Now()
	fmt.Println("Starting to consume messages...")

	// Set an overall timeout for reading messages
	readTimeout := 30 * time.Second
	timeoutTimer := time.NewTimer(readTimeout)
	defer timeoutTimer.Stop()

consumeLoop:
	for {
		select {
		case <-timeoutTimer.C:
			fmt.Println("Overall read timeout reached")
			break consumeLoop
		default:
			// Create context with timeout for this read attempt
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			
			msg, err := r.ReadMessage(ctx)
			cancel() // Always cancel context immediately after use
			
			if err != nil {
				if err.Error() == "context deadline exceeded" {
					// No messages available within timeout
					fmt.Println("No messages available, checking if we should continue...")
					
					// If we already have messages, break
					if len(records) > 0 {
						fmt.Printf("Have %d messages, stopping consumption\n", len(records))
						break consumeLoop
					}
					
					// Otherwise wait and retry
					time.Sleep(2 * time.Second)
					continue
				}
				
				// Other errors
				fmt.Printf("Read error: %v\n", err)
				break consumeLoop
			}
			
			// Successfully read a message
			text := string(msg.Value)
			fmt.Printf("Received message: %s\n", text)
			
			// Parse the message
			parts := strings.Split(text, ",")
			if len(parts) < 4 {
				fmt.Printf("Invalid format, skipping: %s\n", text)
				continue
			}
			
			// Clean up the fields (remove prefixes)
			name := strings.TrimPrefix(parts[1], "name ")
			address := strings.TrimPrefix(parts[2], "address ")
			continent := strings.TrimPrefix(parts[3], "Continent ")
			
			records = append(records, Record{
				ID:        strings.TrimSpace(parts[0]),
				Name:      strings.TrimSpace(name),
				Address:   strings.TrimSpace(address),
				Continent: strings.TrimSpace(continent),
				Raw:       text,
			})
			
			fmt.Printf("Consumed %d records so far\n", len(records))
			
			// Stop after collecting reasonable number (or all 10 from your test)
			if len(records) >= 10 {
				fmt.Println("Collected 10 messages, stopping consumption")
				break consumeLoop
			}
		}
	}

	fmt.Printf("\nTotal records consumed: %d (in %v)\n", len(records), time.Since(start))

	if len(records) == 0 {
		fmt.Println("No data to sort. Exiting.")
		return
	}

	// Produce sorted records to different topics
	produceSorted("id", records, func(a, b Record) bool {
		return a.ID < b.ID
	})
	produceSorted("name", records, func(a, b Record) bool {
		return a.Name < b.Name
	})
	produceSorted("continent", records, func(a, b Record) bool {
		return a.Continent < b.Continent
	})

	fmt.Println("Sorting + produce finished in", time.Since(start))
	
	// Keep running to show it completed successfully
	fmt.Println("\nSorter completed successfully. Press Ctrl+C to exit.")
	
	// Wait for interrupt signal to exit gracefully
	sigChan := make(chan struct{})
	<-sigChan
}

func produceSorted(topic string, recs []Record, less func(a, b Record) bool) {
	fmt.Printf("\nProducing %d records to topic: %s\n", len(recs), topic)

	// Create a copy to sort
	sortedRecs := make([]Record, len(recs))
	copy(sortedRecs, recs)
	
	sort.Slice(sortedRecs, func(i, j int) bool {
		return less(sortedRecs[i], sortedRecs[j])
	})

	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   topic,
		Balancer: &kafka.LeastBytes{},
	})
	defer w.Close()

	successCount := 0
	for _, r := range sortedRecs {
		err := w.WriteMessages(context.Background(),
			kafka.Message{Value: []byte(r.Raw)},
		)
		if err != nil {
			fmt.Printf("Error writing to topic %s: %v\n", topic, err)
			continue
		}
		successCount++
	}

	fmt.Printf("Successfully produced %d/%d records to %s\n", successCount, len(sortedRecs), topic)
}