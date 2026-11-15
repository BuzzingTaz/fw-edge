package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/BuzzingTaz/fw-edge-apps/internal/engine"
	"github.com/joho/godotenv"
	"github.com/nats-io/nats.go"
)

var natsURL *string

func init() {
	godotenv.Load()
	natsURL = flag.String("nats-url", "nats://localhost:4222", "Nats server URL for C2")

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)
}

func main() {
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	nc, err := SetupNATSConnection(*natsURL)
	if err != nil {
		slog.Error("Failed to connect to NATS", "err", err)
		os.Exit(1)
	}
	defer nc.Close()

	orchestrator := engine.NewOrchestrator(nc)
	if err := orchestrator.Start(ctx); err != nil {
		slog.Error("Failed to start orchestrator", "err", err)
		os.Exit(1)
	}

	apiServer := engine.NewAPIServer(orchestrator)
	go func() {
		if err := apiServer.Start(":8080"); err != nil {
			slog.Error("API Server failed", "err", err)
		}
	}()

	slog.Info("Central Engine is running...")

	// Block until context is done
	<-ctx.Done()
	slog.Info("Shutting down Engine...")
}

func SetupNATSConnection(url string) (*nats.Conn, error) {
	opts := []nats.Option{
		nats.Name("Central Engine"),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2 * time.Second),
	}

	nc, err := nats.Connect(url, opts...)
	if err != nil {
		return nil, err
	}
	return nc, nil
}
