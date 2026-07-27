package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/BuzzingTaz/fw-edge-apps/internal/eventsingest"
	pb "github.com/BuzzingTaz/fw-edge-apps/proto"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

var mqttBroker = flag.String("mqtt-broker", "tcp://localhost:1883", "MQTT broker address")
var mqttClient mqtt.Client

type VideoStreamerServiceServer struct {
	pb.UnimplementedVideoStreamServiceServer
}

func (s *VideoStreamerServiceServer) StreamVideo(stream grpc.BidiStreamingServer[pb.EncodedFrame, pb.InferenceResult]) error {
	slog.Info("StreamVideo called")

	streamID := uuid.New().String()
	resultTopic := fmt.Sprintf("compute/results/%s", streamID)
	frameTopic := fmt.Sprintf("compute/frames/%s", streamID)

	// Subscribe to results for this stream
	token := mqttClient.Subscribe(resultTopic, 1, func(client mqtt.Client, msg mqtt.Message) {
		var inferenceResult pb.InferenceResult
		if err := proto.Unmarshal(msg.Payload(), &inferenceResult); err != nil {
			slog.Error("Failed to unmarshal InferenceResult", "err", err)
			return
		}

		eventsingest.TransmitMeasureEvent(time.Now(), "", inferenceResult.TaskId, "scheduler4_result_received_compute", map[string]string{})

		slog.Debug("Received inference results via MQTT", "taskId", inferenceResult.TaskId, "detections", len(inferenceResult.Detections))

		if err := stream.Send(&inferenceResult); err != nil {
			slog.Error("Error sending inference data to client", "err", err)
		} else {
			eventsingest.TransmitMeasureEvent(time.Now(), "", inferenceResult.TaskId, "scheduler4_result_sent", map[string]string{})
		}
	})
	token.Wait()
	if err := token.Error(); err != nil {
		slog.Error("Failed to subscribe to MQTT topic", "err", err)
		return err
	}
	defer func() {
		mqttClient.Unsubscribe(resultTopic)
	}()

	for {
		in, err := stream.Recv()
		if err == io.EOF {
			slog.Info("StreamVideo: client closed the stream cleanly")
			return nil
		}
		if err != nil {
			slog.Error("StreamVideo: error receiving payload from client interface", "err", err)
			return err
		}

		eventsingest.TransmitMeasureEvent(time.Now(), "", in.TaskId, "scheduler4_frame_received", map[string]string{})

		data, err := proto.Marshal(in)
		if err != nil {
			slog.Error("Failed to marshal EncodedFrame", "err", err)
			continue
		}

		token := mqttClient.Publish(frameTopic, 0, false, data)
		token.Wait()
		if err := token.Error(); err != nil {
			slog.Error("Failed to publish frame via MQTT", "err", err)
		} else {
			eventsingest.TransmitMeasureEvent(time.Now(), "", in.TaskId, "scheduler4_frame_sent_compute", map[string]string{})
		}
	}
}

func init() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)
}

func startClientifGRPCServer() {
	lis, err := net.Listen("tcp", ":5000")
	if err != nil {
		log.Fatalf("Failed to listen on gRPC port 5000: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterVideoStreamServiceServer(s, &VideoStreamerServiceServer{})

	log.Println("Scheduler gRPC server listening on :5000")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve gRPC: %v", err)
	}
}

func main() {
	flag.Parse()

	err := eventsingest.Initialize("nats://localhost:4222", "scheduler4")
	if err != nil {
		slog.Warn("Failed to init events ingest client, telemetry will not be tracked", "err", err)
	}
	defer eventsingest.Close()

	opts := mqtt.NewClientOptions()
	opts.AddBroker(*mqttBroker)
	opts.SetClientID(fmt.Sprintf("scheduler4-%s", uuid.New().String()))

	mqttClient = mqtt.NewClient(opts)
	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		slog.Error("Failed to connect to MQTT broker", "err", token.Error())
		os.Exit(1)
	}
	slog.Info("Connected to MQTT broker", "broker", *mqttBroker)

	go startClientifGRPCServer()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	mqttClient.Disconnect(250)
	slog.Info("\nShutdown signal received. Exiting.")
}
