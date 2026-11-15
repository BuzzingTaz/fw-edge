package main

import (
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/BuzzingTaz/fw-edge-apps/internal/clientif"
	"github.com/BuzzingTaz/fw-edge-apps/internal/eventsingest"
	pb "github.com/BuzzingTaz/fw-edge-apps/proto"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
)

var clientsManager *clientif.ClientsManager
var natsURL = "nats://localhost:4222"

var tsToTaskIDMap = make(map[uint64]string) // TODO: Move to within each client, and make ring buffer?
var taskIDToTsMap = make(map[string]uint64)

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
		slog.Error("Failed to create client", "err", err)
		http.Error(w, "Failed to create client: "+err.Error(), http.StatusInternalServerError)
		return
	}

	err = client.ConnectScheduler()
	if err != nil {
		slog.Error("Failed to connect to scheduler", "err", err)
		http.Error(w, "Failed to connect to scheduler: "+err.Error(), http.StatusInternalServerError)
		return
	}
	client.SchedulerListenerHandler = func(message clientif.ProcessedDataMessage) {
		taskID := message.TaskId

		if ts, ok := taskIDToTsMap[taskID]; ok {
			message.Timestamp = ts
		}

		eventsingest.TransmitMeasureEvent(time.Now(), client.UserID, taskID, "clientif_results_reached", map[string]string{})
		slog.Debug("Received processed data from scheduler", "userID", client.UserID, "taskID", message.TaskId)

		if err := clientif.SendDataToClient(client, message); err != nil {
			slog.Error("Failed to send inference data over WebRTC data channel", "error", err)
		}
		eventsingest.TransmitMeasureEvent(time.Now(), client.UserID, taskID, "clientif_results_sent", map[string]string{})
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

	appID := os.Getenv("APP_ID")
	compID := os.Getenv("COMPONENT_ID")
	nodeID := os.Getenv("NODE_ID")
	if nodeID == "" {
		hostname, _ := os.Hostname()
		nodeID = hostname
	}

	centralNats := os.Getenv("CENTRAL_NATS_URL")
	if centralNats != "" {
		natsURL = centralNats
	}
	edgeNats := os.Getenv("EDGE_NATS_URL")
	if edgeNats == "" {
		edgeNats = "nats://127.0.0.1:4223"
	}

	err := eventsingest.Initialize(edgeNats, "clientif_events")
	if err != nil {
		slog.Warn("Failed to init events ingest client, telemetry will not be tracked", "err", err)
	}
	defer eventsingest.Close()

	if appID != "" && compID != "" {
		nc, err := nats.Connect(natsURL)
		if err == nil {
			req := &pb.AppRegistrationRequest{
				AppId:       appID,
				ComponentId: compID,
				NodeId:      nodeID,
			}
			data, _ := proto.Marshal(req)
			msg, err := nc.Request("c2.engine.app_register", data, 2*time.Second)
			if err != nil {
				slog.Error("Failed to register app component with Central Engine", "err", err)
			} else {
				var resp pb.AppRegistrationResponse
				proto.Unmarshal(msg.Data, &resp)
				slog.Info("App component registered with Central Engine", "success", resp.Success)
			}
			nc.Close()
		} else {
			slog.Error("Failed to connect to Central NATS for registration", "err", err)
		}
	}

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
