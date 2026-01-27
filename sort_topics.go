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
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "source",
		GroupID: "sorter",
	})

	defer r.Close()

	var records []Record
	start := time.Now()

	for i := 0; i < 10000; i++ {
		msg, err := r.ReadMessage(context.Background())
		if err != nil {
			panic(err)
		}
		parts := strings.Split(string(msg.Value), ",")
		records = append(records, Record{
			ID:        parts[0],
			Name:      parts[1],
			Address:   parts[2],
			Continent: parts[3],
			Raw:       string(msg.Value),
		})
	}

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
}

func produceSorted(topic string, recs []Record, less func(a, b Record) bool) {
	sort.Slice(recs, func(i, j int) bool {
		return less(recs[i], recs[j])
	})

	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   topic,
	})
	defer w.Close()

	for _, r := range recs {
		err := w.WriteMessages(context.Background(),
			kafka.Message{Value: []byte(r.Raw)},
		)
		if err != nil {
			panic(err)
		}
	}
}
