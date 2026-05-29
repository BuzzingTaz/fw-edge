package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/gorilla/websocket"

	"github.com/BuzzingTaz/fw-edge-apps/internal/clientif"
	"github.com/BuzzingTaz/fw-edge-apps/internal/metrics"
)

// WebRTC signalling message over WebSocket

var clientsManager *clientif.ClientsManager
var upgrader = websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
var natsURL = "localhost:4222"

func wsHandler(w http.ResponseWriter, r *http.Request) {
	// Upgrade HTTP to WebSocket
	userID := r.PathValue("userID")
	slog.Info("Incoming websocket handshake",
	"userID", userID,
	"remote_addr", r.RemoteAddr,
	"user_agent", r.UserAgent(),
	"x_forwarded_for", r.Header.Get("X-Forwarded-For"),
)

	// TODO: Improve validation
	if userID == "" {
		http.Error(w, "userID is required", http.StatusBadRequest)
		return
	}

	client := clientsManager.CreateClient(userID)

	clientConn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Error("WebSocket upgrade failed", "error", err)
		return
	}
	client.ClientConn = clientConn
	slog.Info("WebSocket connection established for ", "userID", userID)

	if err = client.ConnectScheduler(); err != nil {
		slog.Error("Failed to connect to scheduler ", err)
		return
	}
	go func() {
		var message clientif.ProcessedDataMessage
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			err = client.SchedulerConn.ReadJSON(&message)
			if err != nil {
				slog.Error("Failed to read from scheduler", "error", err)
				break
			}
			enrichedCtx := metrics.WithTaskID(ctx, "test_task")

			metrics.TrackMetric(enrichedCtx, "clientif_received_scheduler", map[string]string{
				"time": strconv.FormatUint(message.Timestamp, 10),
			})

			slog.Info("Received processed data from scheduler", "userID", userID, "Frame Timestamp", message.Timestamp)
			if err = client.SendDataToPeer(message); err != nil {
				slog.Error("Failed to send inference data over WebRTC data channel", "error", err)
			}
		}
	}()

	go client.SetupWebRTCSignalHandler()

	if err = client.InitiatePC(); err != nil {
		slog.Error("Failed to establish PeerConnection", "error", err)
		return
	}
}

func init() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	slog.SetDefault(logger)
	clientsManager = clientif.NewClientsManager()
}

func main() { //nolint:gocognit,cyclop,gocyclo,maintidx
	defer clientsManager.CloseAll()
	err := metrics.InitMetrics(natsURL, "clientif_metrics")
	if err != nil {
		slog.Warn("Failed to init metrics, metrics will not be tracked: %v", err)
	}
	defer metrics.CloseMetrics()

	http.HandleFunc("/ws/{userID}", wsHandler)
	go func() {
		slog.Info("Starting server on :9999")
		if err := http.ListenAndServe(":9999", nil); err != nil {
			slog.Error("Server failed", "error", err)
		}
	}()

	// wait
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	fmt.Println("\nShutdown signal received. Exiting.")
}
