package source

import (
	"context"
	"flag"
//	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

var (
	broker   = flag.String("broker", "localhost:9092", "Kafka broker")
	topic    = flag.String("topic", "source", "Kafka topic")
	total    = flag.Int64("total", 5_000_000, "Total records to generate")
	workers  = flag.Int("workers", 4, "Number of workers")
	batch    = flag.Int("batch", 2000, "Kafka batch size")
)

var (
	letters      = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	addressChars = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 ")
	continents   = []string{
		"North America", "Asia", "South America",
		"Europe", "Africa", "Australia",
	}
)

func randFromSet(r *rand.Rand, set []byte, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = set[r.Intn(len(set))]
	}
	return b
}

func producer(){
	flag.Parse()

	start := time.Now()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// signals
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		log.Println("shutdown signal")
		cancel()
	}()

	writer := &kafka.Writer{
		Addr:         kafka.TCP(*broker),
		Topic:        *topic,
		BatchSize:    *batch,
		RequiredAcks: kafka.RequireOne,
		Compression:  kafka.Snappy,
	}
	defer writer.Close()

	var globalID int64
	var sent int64
	var wg sync.WaitGroup

	perWorker := *total / int64(*workers)

	wg.Add(*workers)
	for w := 0; w < *workers; w++ {
		go func(worker int) {
			defer wg.Done()

			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(worker)))
			msgs := make([]kafka.Message, 0, *batch)

			for i := int64(0); i < perWorker; i++ {
				select {
				case <-ctx.Done():
					return
				default:
				}

				id := atomic.AddInt64(&globalID, 1)

				nameLen := 10 + r.Intn(6)
				addrLen := 15 + r.Intn(6)

				record := make([]byte, 0, 64)
				record = strconv.AppendInt(record, id, 10)
				record = append(record, ',')

				record = append(record, randFromSet(r, letters, nameLen)...)
				record = append(record, ',')

				record = append(record, randFromSet(r, addressChars, addrLen)...)
				record = append(record, ',')

				record = append(record, continents[r.Intn(len(continents))]...)

				msgs = append(msgs, kafka.Message{Value: record})

				if len(msgs) == cap(msgs) {
					if err := writer.WriteMessages(ctx, msgs...); err != nil {
						log.Fatalf("write failed: %v", err)
					}
					atomic.AddInt64(&sent, int64(len(msgs)))
					msgs = msgs[:0]
				}
			}

			if len(msgs) > 0 {
				if err := writer.WriteMessages(ctx, msgs...); err != nil {
					log.Fatalf("flush failed: %v", err)
				}
				atomic.AddInt64(&sent, int64(len(msgs)))
			}
		}(w)
	}

	wg.Wait()

	elapsed := time.Since(start)
	log.Printf(
		"done | sent=%d | elapsed=%s | rate=%.0f rows/sec",
		sent,
		elapsed,
		float64(sent)/elapsed.Seconds(),
	)
}
