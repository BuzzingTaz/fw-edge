package main

import (
	"context"
	"flag"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	pb "github.com/BuzzingTaz/fw-edge-apps/proto"
	"github.com/gorilla/websocket"
	"github.com/pion/rtp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var computeAddr = flag.String("compute-addr", "localhost:9997", "the address to connect to")
var computeStreamClient pb.ComputeStreamClient

var tempConn *websocket.Conn
var upgrader = websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}

func wsHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	slog.Info("WebSocket handler called")

	userID := r.PathValue("userID")

	if userID == "" {
		http.Error(w, "userID is required", http.StatusBadRequest)
		return
	}

	tempConn, err = upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Error("WebSocket upgrade failed", "err", err)
		return
	}
	// defer conn.Close()
	slog.Info("WebSocket connection established for user", "userID", userID)

	// go SendFrameData(...)
	go func() {
		var message rtp.Packet

		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
		defer cancel()
		computeVideoStreamer, err := computeStreamClient.StreamVideo(ctx)
		if err != nil {
			slog.Error("Failed to create rpc from client", "err", err)
			return
		}

		go ReadInferenceData(computeVideoStreamer)

		for {
			err = tempConn.ReadJSON(&message)
			if err != nil {
				slog.Error("WebSocket read error", "err", err)
				break
			}

			slog.Debug("Received message", "timestamp", message.Timestamp)
			slog.Debug(" from user", "userID", userID)

			rawBytes, err := message.Marshal()
			if err != nil {
				slog.Error("Error marshaling RTP packet", "err", err)
				continue
			}

			computeVideoStreamer.Send(
				&pb.RTPPacket{Data: rawBytes})

		}
	}()

}

func SendDecodedFrameData(stream grpc.BidiStreamingClient[pb.DecodedFrame, pb.InferenceResult]) {

}

func ReadInferenceData(stream grpc.BidiStreamingClient[pb.RTPPacket, pb.InferenceResult]) {
	for {
		inferenceData, err := stream.Recv()
		if err == io.EOF {
			slog.Info("Server closed the inference stream.")
			return
		}
		if err != nil {
			slog.Error("Error receiving inference data", "err", err)
			return
		}

		slog.Info("Received inference results", "timestamp", inferenceData.Timestamp, "processingStatus", inferenceData.ProcessingStatus, "detections", len(inferenceData.Detections))

		if tempConn != nil {
			err = tempConn.WriteJSON(inferenceData)
			if err != nil {
				slog.Error("Error sending inference data to client", "err", err)
				return
			}
			slog.Debug("Sent inference data back to client for frame", "timestamp", inferenceData.Timestamp)
		} else {
			slog.Warn("No WebSocket connection to send inference data to client.")
		}

	}
}

func init() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)
}

func main() {
	flag.Parse()

	computeUnitConn, err := grpc.NewClient(*computeAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		slog.Error("did not connect to compute", "err", err)
	}
	defer computeUnitConn.Close()

	computeStreamClient = pb.NewComputeStreamClient(computeUnitConn)

	http.HandleFunc("/ws/{userID}", wsHandler)

	slog.Info("Server starting on :9998")
	go func() {
		if err := http.ListenAndServe(":9998", nil); err != nil {
			slog.Error("Failed to start server", "err", err)
		}
	}()

	// frameConsumer := scheduler.NewConsumer(clientID, nats URL, subscribeSubject, queueGroup)
	// go frameConsumer.StartConsuming()

	// frameScheduler := scheduler.NewScheduler(nil, nil, []string{"node1", "node2", "node3"})
	// fmt.Println(frameScheduler)
	// go frameScheduler.Run()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	slog.Info("\nShutdown signal received. Exiting.")
}
