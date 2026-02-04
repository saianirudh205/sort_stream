package main

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	brokerAddr = "localhost:9092"
	topic      = "source"
	total      = 5_000_000
	numWorkers = 20
	batchSize  = 2000
)

var (
	continents   = [...]string{"North America", "Asia", "South America", "Europe", "Africa", "Australia"}
	letters      = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
)

func randString(r *rand.Rand, buf []byte, n int) []byte {
	for i := 0; i < n; i++ {
		buf = append(buf, letters[r.Intn(len(letters))])
	}
	return buf
}

func producer() {
	var wg sync.WaitGroup
	wg.Add(numWorkers)

	start := time.Now()
	var sent int64

	for w := 0; w < numWorkers; w++ {
		go func(worker int) {
			defer wg.Done()

			writer := &kafka.Writer{
				Addr:         kafka.TCP(brokerAddr),
				Topic:        topic,
				BatchSize:    batchSize,
				BatchTimeout: 10 * time.Millisecond,
				Async:        true,
				RequiredAcks: 0,
				Compression:  kafka.Snappy,
				Balancer:     &kafka.LeastBytes{},
			}
			defer writer.Close()

			r := rand.New(rand.NewSource(int64(worker)))
			msgs := make([]kafka.Message, 0, batchSize)
			var buf []byte

			for i := 0; i < total/numWorkers; i++ {
				id := 1 + r.Int31n(5_100_000)

				buf = buf[:0]
				buf = strconv.AppendInt(buf, int64(id), 10)
				buf = append(buf, ',')
				buf = randString(r, buf, 10) // name
				buf = append(buf, ',')
				buf = randString(r, buf, 15) // address
				buf = append(buf, ',')
				buf = append(buf, continents[r.Intn(len(continents))]...)

				val := make([]byte, len(buf))
				copy(val, buf)

				msgs = append(msgs, kafka.Message{Value: val})

				if len(msgs) == batchSize {
					if err := writer.WriteMessages(context.Background(), msgs...); err != nil {
						panic(fmt.Sprintf("worker %d: write failed: %v", worker, err))
					}
					atomic.AddInt64(&sent, int64(len(msgs)))
					msgs = msgs[:0]
				}
			}

			if len(msgs) > 0 {
				if err := writer.WriteMessages(context.Background(), msgs...); err != nil {
					panic(fmt.Sprintf("worker %d: flush failed: %v", worker, err))
				}
				atomic.AddInt64(&sent, int64(len(msgs)))
			}
		}(w)
	}

	wg.Wait()
	elapsed := time.Since(start).Seconds()
	finalSent := atomic.LoadInt64(&sent)
	fmt.Println("Sent:", finalSent, "rate:", int(float64(finalSent)/elapsed), "msg/sec")
}

func main() {
	producer()
}