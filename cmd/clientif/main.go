package main

import (
	"log/slog"
	"net/http"
	"os"

	"github.com/gorilla/websocket"

	"github.com/BuzzingTaz/fw-edge-apps/internal/clientif"
)

// WebRTC signalling message over WebSocket

var clientsManager = clientif.NewClientsManager()
var upgrader = websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}

func wsHandler(w http.ResponseWriter, r *http.Request) {
	// Upgrade HTTP to WebSocket
	userID := r.PathValue("userID")

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
		for {
			err = client.SchedulerConn.ReadJSON(&message)
			if err != nil {
				slog.Error("Failed to read from scheduler", "error", err)
				return
			}

			slog.Info("Received processed data from scheduler", "userID", userID, "data", message)
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
}

func main() { //nolint:gocognit,cyclop,gocyclo,maintidx
	defer clientsManager.CloseAll()

	http.HandleFunc("/ws/{userID}", wsHandler)

	slog.Info("Starting server on :9999")
	if err := http.ListenAndServe(":9999", nil); err != nil {
		slog.Error("Server failed", "error", err)
	}

}
