package main

import (
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/BuzzingTaz/fw-edge-apps/internal/clientif"
	"github.com/BuzzingTaz/fw-edge-apps/internal/metrics"
)

var clientsManager *clientif.ClientsManager
var natsURL = "localhost:4222"

var tsToTaskIDMap = make(map[uint64]string) // TODO: Move to within each client, and make ring buffer?

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
	client.SchedulerListenerHandler = func(message clientif.ProcessedDataMessage) {
		taskID := tsToTaskIDMap[message.Timestamp]
		metrics.SampleEvent(time.Now(), client.UserID, taskID, "clientif_results_reached", map[string]string{
			"timestamp": strconv.FormatUint(message.Timestamp, 10),
		})
		slog.Debug("Received processed data from scheduler", "userID", client.UserID, "Frame Timestamp", message.Timestamp)

		if err := clientif.SendDataToClient(client, message); err != nil {
			slog.Error("Failed to send inference data over WebRTC data channel", "error", err)
		}
		metrics.SampleEvent(time.Now(), client.UserID, taskID, "clientif_results_sent", map[string]string{
			"timestamp": strconv.FormatUint(message.Timestamp, 10),
		})
	}
	go client.ListenScheduler()

	if protocol == "webrtc" {
		slog.Info("Initiating WebRTC connection for ", "userID", client.UserID)
		HandleWebRTC(client, w, r)
	}
}

func init() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)
	clientsManager = clientif.NewClientsManager()
}

func main() { //nolint:gocognit,cyclop,gocyclo,maintidx
	defer clientsManager.CloseAll()
	err := metrics.InitMetricsClient(natsURL, "clientif_metrics")
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
