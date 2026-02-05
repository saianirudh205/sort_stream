package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"app/source"
	"app/sort"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 2)

	go func() {
		log.Println("starting source")
		errCh <- source.Run(ctx)
	}()

	go func() {
		log.Println("starting sort")
		errCh <- sort.Run(ctx)
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		log.Println("received signal:", sig)
		cancel()

	case err := <-errCh:
		log.Println("service error:", err)
		cancel()
	}

	<-errCh // wait for other goroutine
	log.Println("container exiting")
}
