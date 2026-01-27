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

func main() {
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "source",
	})

	defer w.Close()
	rand.Seed(time.Now().UnixNano())

	start := time.Now()

	for i := 0; i < 10000; i++ { // keep small for demo
		line := fmt.Sprintf(
			"%d,name%06d,addr %d street,%s",
			rand.Int31(),
			rand.Intn(999999),
			rand.Intn(9999),
			continents[rand.Intn(len(continents))],
		)

		err := w.WriteMessages(context.Background(),
			kafka.Message{Value: []byte(line)},
		)
		if err != nil {
			panic(err)
		}
	}

	fmt.Println("Source finished in", time.Since(start))
}
