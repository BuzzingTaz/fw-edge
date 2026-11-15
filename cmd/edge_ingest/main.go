package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/BuzzingTaz/fw-edge-apps/internal/storage"
	pb "github.com/BuzzingTaz/fw-edge-apps/proto"
	"github.com/joho/godotenv"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
)

const (
	MaxBatchSize  = 1000
	FlushInterval = 2 * time.Second
)

var (
	edgeNatsURL    *string
	centralNatsURL *string
	dbPath         *string
)

type EdgeIngestServer struct {
	edgeNC    *nats.Conn
	centralNC *nats.Conn
	store     storage.TelemetryStorage

	mu    sync.Mutex
	batch []*pb.MeasureEvent
}

func init() {
	godotenv.Load()
	edgeNatsURL = flag.String("edge-nats-url", "nats://localhost:4222", "Edge Nats server URL for telemetry")
	centralNatsURL = flag.String("central-nats-url", "nats://localhost:4223", "Central Nats server URL for summaries")
	dbPath = flag.String("db-path", "telemetry.db", "Path to SQLite database file")

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)
}

func main() {
	flag.Parse()
	ctx := context.Background()

	// Connect to Edge NATS
	edgeNC, err := nats.Connect(*edgeNatsURL, nats.Name("Edge Ingest (Local)"), nats.RetryOnFailedConnect(true))
	if err != nil {
		slog.Error("Failed to connect to Edge NATS", "err", err)
		os.Exit(1)
	}
	defer edgeNC.Close()

	// Connect to Central NATS
	centralNC, err := nats.Connect(*centralNatsURL, nats.Name("Edge Ingest (Central)"), nats.RetryOnFailedConnect(true))
	if err != nil {
		slog.Error("Failed to connect to Central NATS (summaries might not be sent)", "err", err)
		// We don't fail here because local ingest is the priority
	} else {
		defer centralNC.Close()
	}

	// Initialize Storage
	store, err := storage.NewSQLiteStorage(*dbPath)
	if err != nil {
		slog.Error("Failed to initialize SQLite storage", "err", err)
		os.Exit(1)
	}
	defer store.Close()

	server := &EdgeIngestServer{
		edgeNC:    edgeNC,
		centralNC: centralNC,
		store:     store,
	}

	eventChan := make(chan *pb.MeasureEvent, MaxBatchSize*2)

	// Subscribe to telemetry on Edge NATS
	if _, err := edgeNC.Subscribe("events.measure", func(msg *nats.Msg) {
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
	}); err != nil {
		slog.Error("Failed to subscribe to events.measure", "err", err)
		os.Exit(1)
	}

	server.setupHTTPServer()
	slog.Info("Edge Ingest Server Started. Waiting for events...")

	ticker := time.NewTicker(FlushInterval)
	defer ticker.Stop()

	// Optionally set up a ticker for uploading summaries to Central Engine
	summaryTicker := time.NewTicker(1 * time.Minute)
	defer summaryTicker.Stop()

	sigCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	for {
		select {
		case event := <-eventChan:
			server.enqueue(ctx, event)

		case <-ticker.C:
			server.flushPending(ctx)

		case <-summaryTicker.C:
			// In a real scenario, this would aggregate data from SQLite and push via centralNC
			if server.centralNC != nil && server.centralNC.IsConnected() {
				slog.Info("Uploading telemetry summary to Central Engine (simulated)")
				// server.centralNC.Publish("c2.engine.telemetry.summary", ...)
			}

		case <-sigCtx.Done():
			slog.Info("Shutting down Edge Ingest, flushing remaining events...")
			server.flushPending(ctx)
			return
		}
	}
}

func (s *EdgeIngestServer) enqueue(ctx context.Context, event *pb.MeasureEvent) {
	s.mu.Lock()
	s.batch = append(s.batch, event)
	full := len(s.batch) >= MaxBatchSize
	s.mu.Unlock()
	if full {
		s.flushPending(ctx)
	}
}

func (s *EdgeIngestServer) flushPending(ctx context.Context) {
	s.mu.Lock()
	batch := s.batch
	s.batch = nil
	s.mu.Unlock()
	if len(batch) == 0 {
		return
	}
	s.flushBatch(ctx, batch)
}

func (s *EdgeIngestServer) flushBatch(ctx context.Context, batch []*pb.MeasureEvent) {
	err := s.store.InsertMeasureEvents(ctx, batch)
	if err != nil {
		slog.Error("Failed to insert measurements", "err", err)
		return
	}
	slog.Debug("Successfully inserted measurements into SQLite database", "count", len(batch))
}
