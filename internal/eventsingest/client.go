package eventsingest

import (
	"fmt"
	"log"
	"sync"
	"time"

	pb "github.com/BuzzingTaz/fw-edge-apps/proto"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	eventSubj             = "events.measure"
	ClientEventBufferSize = 5000
)

type Client struct {
	nc          *nats.Conn
	serviceName string
	eventChan   chan *pb.MeasureEvent
	wg          sync.WaitGroup
}

var defaultClient *Client

func Initialize(natsURL, serviceName string) error {
	if defaultClient != nil {
		return nil // Already initialized
	}

	client, err := NewEventsClient(natsURL, serviceName)
	if err != nil {
		return err
	}
	defaultClient = client
	return nil
}

func Close() {
	if defaultClient != nil {
		defaultClient.Close()
		defaultClient = nil
	}
}

// TransmitMeasureEvent acts as a transparent wrapper around the global client.
func TransmitMeasureEvent(measTime time.Time, userId, taskId, eventType string, payload map[string]string) {
	if defaultClient == nil {
		log.Println("[Events Ingest] Warning: Transmit called before InitGlobal")
		return
	}

	// Delegate to the background worker
	defaultClient.TransmitMeasureEvent(measTime, userId, taskId, eventType, payload)
}

// NewEventsClient establishes the NATS connection and starts the background worker.
func NewEventsClient(natsURL, serviceName string) (*Client, error) {
	var err error
	nc, err := nats.Connect(natsURL,
		nats.Name(fmt.Sprintf("%s_EventsClient", serviceName)),
		nats.RetryOnFailedConnect(true),
		nats.ReconnectWait(2*time.Second),
		nats.MaxReconnects(2),

		// Will log every time the connection drops and why
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			log.Printf("[Events Client] Disconnected from NATS. Reason: %v", err)
		}),

		// Will log exactly when the client gives up permanently
		nats.ClosedHandler(func(nc *nats.Conn) {
			log.Printf("[Events Client] NATS connection permanently CLOSED. Last error: %v", nc.LastError())
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	log.Printf("[Events Client] Connected to NATS at %s", natsURL)

	client := &Client{
		nc:          nc,
		serviceName: serviceName,
		eventChan:   make(chan *pb.MeasureEvent, ClientEventBufferSize),
	}

	client.wg.Add(1)
	go client.workerLoop()

	return client, nil
}

// workerLoop continuously reads from the event buffer, marshals the data,
// and publishes it to NATS.
func (c *Client) workerLoop() {
	defer c.wg.Done()

	// This loop will run until eventChan is explicitly closed in Close()
	for event := range c.eventChan {
		marshaled, err := proto.Marshal(event)
		if err != nil {
			log.Printf("[Events Client] Failed to marshal protobuf: %v", err)
			continue
		}

		if err := c.nc.Publish(eventSubj, marshaled); err != nil {
			log.Printf("[Events Client] Failed to publish event: %v", err)
		}
	}
}

// Close signals the worker to finish processing the buffer, waits for it
// to complete, and safely drains the NATS connection.
func (c *Client) Close() {
	// Closing the channel breaks the range loop in workerLoop
	close(c.eventChan)

	// Wait for the worker to finish processing remaining items in the buffer
	c.wg.Wait()

	if c.nc != nil {
		c.nc.Drain()
	}
}

// TransmitMeasureEvent serializes and publishes the telemetry data.
// It executes synchronously; the underlying nats.Publish handles buffering
// and network I/O efficiently.
func (c *Client) TransmitMeasureEvent(measTime time.Time, userId, taskId, eventType string, payload map[string]string) {
	measureEvent := &pb.MeasureEvent{
		MeasTime:    timestamppb.New(measTime),
		UserId:      userId,
		TaskId:      taskId,
		ServiceName: c.serviceName,
		EventType:   eventType,
		Payload:     payload,
	}

	select {
	case c.eventChan <- measureEvent:
		// Successfully queued
	default:
		log.Printf("[Events Client] Warning: Event buffer full, dropping telemetry for task %s to prevent application blocking", taskId)
	}
}
