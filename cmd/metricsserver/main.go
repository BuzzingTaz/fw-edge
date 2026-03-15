package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	pb "github.com/BuzzingTaz/fw-edge-apps/proto"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
)

// TODO: needs to be persistent
type MetricsStore struct {
	mu    sync.Mutex
	count map[string]int
}

func main() {
	natsURL := "localhost:4222"
	if natsURL == "" {
		natsURL = nats.DefaultURL
	}

	nc, err := nats.Connect(natsURL)
	if err != nil {
		log.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Close()

	store := &MetricsStore{count: make(map[string]int)}

	_, err = nc.Subscribe("metrics.events", func(msg *nats.Msg) {
		var metric pb.MetricEvent
		if err := proto.Unmarshal(msg.Data, &metric); err != nil {
			log.Printf("Error unmarshaling metric: %v", err)
			return
		}

		// Accumulate logic
		store.mu.Lock()
		store.count[metric.EventType]++
		total := store.count[metric.EventType]
		store.mu.Unlock()

		t := time.Unix(int64(metric.Timestamp), 0)
		log.Printf("[Server] Event: %s | Service: %s | Time: %s | Total: %d",
			metric.EventType,
			metric.ServiceName,
			t.Format(time.RFC3339),
			total,
		)
	})
	if err != nil {
		log.Fatalf("Failed to subscribe: %v", err)
	}

	log.Println("[Server] Metrics Server Started. Waiting for events...")

	// Graceful Shutdown
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
	log.Println("[Server] Shutting down...")
}
