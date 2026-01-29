package main

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/segmentio/kafka-go"
)

var continents = []string{
	"North America", "Asia", "South America",
	"Europe", "Africa", "Australia",
}

// Simple random string generator
func randomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	
	// Seed the random number generator
	rand.Seed(time.Now().UnixNano())
	
	// Create a byte slice to hold the result
	result := make([]byte, length)
	
	// Fill with random characters from charset
	for i := range result {
		result[i] = charset[rand.Intn(len(charset))]
	}
	
	return string(result)
}

func main() {
	fmt.Println("Started Source 21")
	
	// SINGLE declaration of w (remove the duplicate)
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      []string{"localhost:9092"},
		Topic:        "source",
		RequiredAcks: 1, // Use 1 for leader, or -1 for all (RequireAll)
		Async:        false, // Synchronous writes
	})
	
	defer w.Close()
	rand.Seed(time.Now().UnixNano())

	start := time.Now()
	fmt.Println("before for")
	for i := 0; i < 10; i++ { // keep small for demo
		line := fmt.Sprintf(
			"%d,name %s,address %s,Continent %s",
			rand.Int31(),
			randomString(10),
			randomString(20),
			continents[rand.Intn(len(continents))],
		)
		
		fmt.Println("before write ==>",line)
		err := w.WriteMessages(context.Background(),
			kafka.Message{Value: []byte(line)},
		)
		if err != nil {
			panic(err)
		}
	}

	fmt.Println("Source finished in", time.Since(start))

	
}