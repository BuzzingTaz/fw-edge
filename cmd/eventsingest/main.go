package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"sync"

	pb "github.com/BuzzingTaz/fw-edge-apps/proto"
	"github.com/joho/godotenv"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
)

const (
	MaxBatchSize  = 1000
	FlushInterval = 2 * time.Second
)

type EventsIngestServer struct {
	nc      *nats.Conn
	db      *DBManager
	tracker *ClientTracker
}

type ClientTracker struct {
	mu      sync.RWMutex
	clients map[string]*ClientInfo
}

type ClientInfo struct {
	ServiceID   string
	CurrentTask string
	LastSeen    time.Time
	TotalEvents uint64
	ErrorCount  uint64
}

var natsURL *string
var serverState EventsIngestServer

func init() {
	godotenv.Load()
	natsURL = flag.String("nats-url", "nats:4222", "Nats server URL")
	serverState = EventsIngestServer{tracker: &ClientTracker{clients: make(map[string]*ClientInfo)}}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)
}

func main() {
	flag.Parse()
	ctx := context.Background()

	nc, err := SetupNATSConnection(*natsURL)
	if err != nil {
		slog.Error("Failed to connect to NATS", "err", err)
	}
	defer nc.Close()
	serverState.nc = nc

	dbCfg := DBConfig{
		ConnectionString: fmt.Sprintf("postgresql://%s:%s@%s/%s?sslmode=disable", os.Getenv("TSDB_USER"), os.Getenv("TSDB_PASSWORD"), "db:5432", "postgres"),
		MaxRetries:       5,
		RetryInterval:    3 * time.Second,
	}
	dbManager, err := NewDBManager(ctx, dbCfg)
	if err != nil {
		slog.Error("Database initialization failed", "err", err)
	}
	defer dbManager.Close()
	serverState.db = dbManager

	eventChan := make(chan *pb.MeasureEvent, MaxBatchSize*2)

	_, err = nc.Subscribe("events.measure", func(msg *nats.Msg) {
		var measurement pb.MeasureEvent
		if err := proto.Unmarshal(msg.Data, &measurement); err != nil {
			slog.Error("Error unmarshaling data", "err", err)
			return
		}

		select {
		case eventChan <- &measurement:
		default:
			slog.Warn("Event channel full, dropping message to prevent backpressure")
		}

		t := time.Unix(measurement.GetMeasTime().Seconds, 0)
		slog.Info("Received event", "EventType", measurement.EventType, "ServiceName", measurement.ServiceName, "Time", t.Format(time.RFC3339))
	})
	if err != nil {
		slog.Error("Failed to subscribe", "err", err)
	}

	setupHTTPServer()

	slog.Info("Measurements Ingest Server Started. Waiting for events...")


	ticker := time.NewTicker(FlushInterval)
	defer ticker.Stop()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var currentBatch []*pb.MeasureEvent

	for {
		select {
		case event := <-eventChan:
			currentBatch = append(currentBatch, event)

			if len(currentBatch) >= MaxBatchSize {
				flushBatchToDB(currentBatch)
				currentBatch = nil // Clear the slice after flushing
			}

		case <-ticker.C:
			if len(currentBatch) > 0 {
				flushBatchToDB(currentBatch)
				currentBatch = nil // Clear the slice after flushing
			}
		case <-ctx.Done():
			slog.Info("Shutting down, flushing remaining events...")
			if len(currentBatch) > 0 {
				flushBatchToDB(currentBatch)
			}
			return
		}
	}
}

func flushBatchToDB(batch []*pb.MeasureEvent) {
	err := serverState.db.InsertMeasureEvents(context.Background(), batch)
	if err != nil {
		slog.Error("Failed to insert measurements","err", err)
		return
	}
	slog.Info("Successfully inserted measurements into database", "count", len(batch))
}

func SetupNATSConnection(url string) (*nats.Conn, error) {
	opts := []nats.Option{
		nats.Name("Events Ingest Service"),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2 * time.Second),

		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			if err != nil {
				slog.Error("Disconnected from NATS due to error", "err", err)
			} else {
				slog.Info("Disconnected from NATS natively")
			}
		}),

		nats.ReconnectHandler(func(nc *nats.Conn) {
			slog.Info("Successfully reconnected to NATS server", "URL", nc.ConnectedUrl())
		}),

		nats.ClosedHandler(func(nc *nats.Conn) {
			slog.Info("NATS connection completely closed")
		}),
	}

	// Connect using the configured options
	nc, err := nats.Connect(url, opts...)
	if err != nil {
		return nil, err
	}

	return nc, nil
}
