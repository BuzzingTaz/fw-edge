package main

import (
	"log/slog"
	"net/http"
	"os"

	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v4"

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

	schedulerConn, _, err := websocket.DefaultDialer.Dial("ws://localhost:9998/ws/scheduler", nil)
	if err != nil {
		slog.Error("Failed to connect to scheduler", "error", err)
		return
	}
	client.SchedulerConn = schedulerConn
	slog.Info("Connected to scheduler WebSocket for ", "userID", userID)

	if err = client.EstablishPC(); err != nil {
		slog.Error("Failed to establish PeerConnection", "error", err)
		return
	}

	var message clientif.ClientWsMessage
	go func() {
		for {
			err = clientConn.ReadJSON(&message)
			if err != nil {
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					slog.Error("WS Error", "err", err)
				}
				break
			}

			if err != nil {
				slog.Error("Failed to connect to scheduler", "error", err)
				return
			}

			if message.Signal != nil {
				slog.Info("Received WebRTC signal", "type", message.Signal.Type)
				messageSignal := message.Signal

				switch messageSignal.Type {
				case "answer":
					slog.Info("Received answer from client", "userID", userID)
					answer := webrtc.SessionDescription{
						Type: webrtc.SDPTypeAnswer,
						SDP:  messageSignal.SDP,
					}
					if err = client.PeerConnection.SetRemoteDescription(answer); err != nil {
						slog.Error("SetRemoteDescription failed", "error", err)
					} else {
						slog.Info("Set remote description with answer", "userID", userID)
					}
				case "candidate":
					slog.Info("Received ICE candidate from client", "userID", userID)
					if messageSignal.ICE != nil {
						if err = client.PeerConnection.AddICECandidate(*messageSignal.ICE); err != nil {
							slog.Error("AddICECandidate failed", "error", err)
						} else {
							slog.Info("Added ICE candidate", "userID", userID)
						}
					} else {
						slog.Error("Received nil ICE candidate", "userID", userID)
					}
				default:
					slog.Warn("Unknown message type", "type", messageSignal.Type)
				}
			}

		}
	}()
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
