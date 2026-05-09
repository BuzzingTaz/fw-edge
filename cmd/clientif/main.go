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

var clientsManager *clientif.ClientsManager
var natsURL = "localhost:4222"

func initiateHandler(w http.ResponseWriter, r *http.Request) {
	protocol := r.PathValue("protocol")
	if !clientif.IsValidProtocol(protocol) {
		http.Error(w, "Invalid protocol", http.StatusBadRequest)
		return
	}

	userID := r.PathValue("userID")
	if userID == "" {
		// TODO: Improve validation
		http.Error(w, "userID is required", http.StatusBadRequest)
		return
	}

	client, err := clientsManager.CreateClient(userID, protocol)
	if err != nil {
		slog.Error("Failed to create client ", err)
		http.Error(w, "Failed to create client: "+err.Error(), http.StatusInternalServerError)
		return
	}

	err = client.ConnectScheduler()
	if err != nil {
		slog.Error("Failed to connect to scheduler ", err)
		http.Error(w, "Failed to connect to scheduler: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if protocol == "webrtc" {
		slog.Info("Initiating WebRTC connection for ", "userID", userID)

		var upgrader = websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
		clientConn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			slog.Error("WebSocket upgrade failed", "error", err)
			return
		}
		client.ClientConn = clientConn
		slog.Info("Signalling WebSocket connection established for ", "userID", userID)

		go client.ListenWebRTCSignalHandler()

		go clientif.ListenScheduler(client, SchedulerCallback) // Go doesn't have generics for methods

		if err := client.InitiatePC(); err != nil {
			slog.Error("Failed to establish PeerConnection", "error", err)
			return
		}
	}
}

func SchedulerCallback(client *clientif.Client, message clientif.ProcessedDataMessage) {
	ctx := context.Background()

	enrichedCtx := metrics.WithTaskID(ctx, "test_task")

	metrics.TrackMetric(enrichedCtx, "clientif_received_scheduler", map[string]string{
		"time": strconv.FormatUint(message.Timestamp, 10),
	})

	slog.Info("Received processed data from scheduler", "userID", client.UserID, "Frame Timestamp", message.Timestamp)
	if err := clientif.SendDataToPeer(client, message); err != nil {
		slog.Error("Failed to send inference data over WebRTC data channel", "error", err)
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

	http.HandleFunc("/initiate/{protocol}/{userID}", initiateHandler)
	go func() {
		slog.Info("Starting server on :9999")
		if err := http.ListenAndServe(":9999", nil); err != nil {
			slog.Error("Server failed", "error", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	fmt.Println("\nShutdown signal received. Exiting.")
}
