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

	pb "github.com/BuzzingTaz/fw-edge-apps/proto"
	"github.com/joho/godotenv"
	"github.com/nats-io/nats.go"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/mem"
	"google.golang.org/protobuf/proto"
)

var (
	natsURL  *string
	nodeID   *string
	interval *time.Duration
)

func init() {
	godotenv.Load()
	natsURL = flag.String("nats-url", "nats://localhost:4222", "Edge Nats server URL")
	nodeID = flag.String("node-id", os.Getenv("NODE_ID"), "Unique ID of this edge node")
	interval = flag.Duration("interval", 1*time.Second, "Interval to publish metrics")

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)
}

func main() {
	flag.Parse()

	if *nodeID == "" {
		hostname, _ := os.Hostname()
		if hostname == "" {
			hostname = "unknown-node"
		}
		*nodeID = hostname
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	nc, err := nats.Connect(*natsURL, nats.Name("Edge Resource Monitor"), nats.RetryOnFailedConnect(true), nats.MaxReconnects(-1))
	if err != nil {
		slog.Error("Failed to connect to NATS", "err", err)
		os.Exit(1)
	}
	defer nc.Close()

	slog.Info("Resource Monitor started", "node_id", *nodeID, "interval", *interval)

	ticker := time.NewTicker(*interval)
	defer ticker.Stop()

	// Initial CPU times to get percentage later
	cpu.Percent(0, false)

	for {
		select {
		case <-ticker.C:
			metrics := collectMetrics()
			publishMetrics(nc, metrics)
		case <-ctx.Done():
			slog.Info("Shutting down Resource Monitor...")
			return
		}
	}
}

func collectMetrics() *pb.ResourceMetrics {
	metrics := &pb.ResourceMetrics{
		NodeId: *nodeID,
	}

	// Get CPU
	cpuPercents, err := cpu.Percent(0, false)
	if err == nil && len(cpuPercents) > 0 {
		metrics.CpuPercent = float32(cpuPercents[0])
	}

	// Get Memory
	v, err := mem.VirtualMemory()
	if err == nil {
		metrics.MemoryPercent = float32(v.UsedPercent)
	}

	return metrics
}

func publishMetrics(nc *nats.Conn, metrics *pb.ResourceMetrics) {
	data, err := proto.Marshal(metrics)
	if err != nil {
		slog.Error("Failed to marshal metrics", "err", err)
		return
	}

	// Publish to the edge nats broker. Wait, the plan says it publishes to Edge NATS,
	// but the Central Engine needs these metrics to decide offloading, 
	// or maybe edge_actor forwards them?
	// The Orchestrator subscribes to "c2.engine.metrics". 
	// In the real world, maybe edge_ingest forwards it. 
	// For simplicity, if we have dual brokers, we might publish to edge broker, 
	// and edge_ingest bridges it to central broker.
	// Let's publish to "edge.metrics.resource" locally.
	topic := fmt.Sprintf("edge.metrics.resource.%s", *nodeID)
	if err := nc.Publish(topic, data); err != nil {
		slog.Error("Failed to publish metrics", "err", err)
	}
}
