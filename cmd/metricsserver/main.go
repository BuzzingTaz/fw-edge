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

var eventsBuffer []*pb.MeasureEvent

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

	pool, err := ConnectDB()
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer pool.Close()

	store := &MetricsStore{count: make(map[string]int)}

	_, err = nc.Subscribe("metrics.events", func(msg *nats.Msg) {
		var measurement pb.MeasureEvent
		if err := proto.Unmarshal(msg.Data, &measurement); err != nil {
			log.Printf("Error unmarshaling metric: %v", err)
			return
		}

		// Accumulate logic
		store.mu.Lock()
		store.count[measurement.EventType]++
		total := store.count[measurement.EventType]
		store.mu.Unlock()

		eventsBuffer = append(eventsBuffer, &measurement)

		t := time.Unix(measurement.GetMeasTime().Seconds, 0)
		log.Printf("[Server] Event: %s | Service: %s | Time: %s | Total: %d",
			measurement.EventType,
			measurement.ServiceName,
			t.Format(time.RFC3339),
			total,
		)
	})
	if err != nil {
		log.Fatalf("Failed to subscribe: %v", err)
	}

	log.Println("[Server] Metrics Server Started. Waiting for events...")

	ticker := time.NewTicker(2 * time.Second)

	// Graceful Shutdown
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	for {
		select {
		case <-ticker.C:
			err := InsertMeasurements(context.Background(), pool, eventsBuffer) // TODO: Clear eventsBuffer after
			if err != nil {
				log.Printf("[Server] Failed to insert measurements: %v", err)
			}
			log.Printf("[Server] Inserted %d measurements into database", len(eventsBuffer))
		case <-ctx.Done():
			log.Println("[Server] Shutting down...")
			return
		}
	}
}
