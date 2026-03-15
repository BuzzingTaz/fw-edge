package metrics

import (
	"context"
	"log"
	"sync"
	"time"

	pb "github.com/BuzzingTaz/fw-edge-apps/proto"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
)

var (
	nc          *nats.Conn
	serviceName string
	metricsSubj = "metrics.events"
	initOnce    sync.Once
)

// InitMetrics establishes the connection to NATS.
// This should be called once during application startup.
func InitMetrics(natsURL, name string) error {
	var err error
	initOnce.Do(func() {
		serviceName = name
		nc, err = nats.Connect(natsURL,
			nats.ReconnectWait(2*time.Second),
			nats.MaxReconnects(10),
		)
		if err != nil {
			return
		}
		log.Printf("[Metrics] Connected to NATS at %s", natsURL)
	})
	return err
}

func CloseMetrics() {
	if nc != nil {
		nc.Drain()
	}
}

// Transmit metric to metrics server through nats
// Non blocking
func TrackMetric(ctx context.Context, eventType string, data map[string]string) {
	if nc == nil || !nc.IsConnected() {
		return
	}

	go func() {
		select {
		case <-ctx.Done():
			return
		default:
		}

		metric := &pb.MetricEvent{
			TaskId:      uuid.NewString(), // TODO: Take from ctx
			ServiceName: serviceName,
			Timestamp:   uint64(time.Now().Unix()), // TODO: Take from ctx maybe?
			EventType:   eventType,
			Data:        data,
		}

		marshaled, err := proto.Marshal(metric)
		if err != nil {
			log.Printf("[Metrics] Failed to marshal protobuf: %v", err)
			return
		}

		if err := nc.Publish(metricsSubj, marshaled); err != nil {
			log.Printf("[Metrics] Failed to publish metric: %v", err)
		}
	}()
}
