package metrics

import (
	"log"
	"sync"
	"time"

	pb "github.com/BuzzingTaz/fw-edge-apps/proto"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TODO: Implement a client struct instead
var (
	nc          *nats.Conn
	serviceName string
	metricsSubj = "metrics.events"
	initOnce    sync.Once
)

// InitMetricsClient establishes the connection to NATS.
// This should be called once during application startup.
func InitMetricsClient(natsURL, name string) error {
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
func SampleEvent(measTime time.Time, userId string, taskId string, eventType string, payload map[string]string) {
	if nc == nil || !nc.IsConnected() {
		return
	}

	go func() {
		metric := &pb.MeasureEvent{
			MeasTime:    	timestamppb.New(measTime),
			UserId: 	 	userId,
			TaskId:      	taskId,
			ServiceName: 	serviceName,
			EventType:   	eventType,
			Payload:        payload,
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
