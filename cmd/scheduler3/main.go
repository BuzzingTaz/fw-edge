package main

import (
	"context"
	"flag"
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
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var computeAddr = flag.String("compute-addr", "localhost:9997", "the address to connect to")
var computeStreamClient pb.ComputeStreamClient

type VideoStreamerServiceServer struct {
	pb.UnimplementedVideoStreamServiceServer
}

// StreamVideo handles bidirectional streaming of video frames and inference results between the client and the scheduler engine.
func (s *VideoStreamerServiceServer) StreamVideo(stream grpc.BidiStreamingServer[pb.EncodedFrame, pb.InferenceResult]) error {
	slog.Info("StreamVideo called")

	ctx, cancel := context.WithTimeout(stream.Context(), 300*time.Second)
	defer cancel()

	computeVideoStreamer, err := computeStreamClient.StreamEncodedFrames(ctx)
	if err != nil {
		slog.Error("Failed to create compute stream", "err", err)
		return err
	}

	frameChan := make(chan *pb.EncodedFrame, 64)

	// Worker 1: Read inference results from compute engine and send back to client
	go ReadInferenceData(stream, computeVideoStreamer)

	// Worker 2: Safe async processing loop to forward data to the compute stream
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case frame, ok := <-frameChan:
				if !ok {
					return
				}
				if err := computeVideoStreamer.Send(frame); err != nil {
					slog.Error("Error forwarding encoded frame down to compute layer", "err", err)
					return
				}
				eventsingest.TransmitMeasureEvent(time.Now(), "", frame.TaskId, "scheduler3_frame_sent_compute", map[string]string{})
			}
		}
	}()

	// Main Loop: Drain client incoming frames immediately without blocking
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			slog.Info("StreamVideo: client closed the stream cleanly")
			close(frameChan)
			return nil
		}
		if err != nil {
			slog.Error("StreamVideo: error receiving payload from client interface", "err", err)
			close(frameChan)
			return err
		}

		eventsingest.TransmitMeasureEvent(time.Now(), "", in.TaskId, "scheduler3_frame_received", map[string]string{})

		// Push to queue immediately. Loop stays open to continue execution.
		select {
		case frameChan <- in:
		default:
			slog.Warn("Scheduler frame channel buffer saturated. Dropping frame packet to prevent pipeline lock.")
		}
	}
}

func ReadInferenceData(
	clientStream grpc.BidiStreamingServer[pb.EncodedFrame, pb.InferenceResult],
	computeStream grpc.BidiStreamingClient[pb.EncodedFrame, pb.InferenceResult],
) {
	for {
		inferenceData, err := computeStream.Recv()
		if err == io.EOF {
			slog.Info("Compute closed the inference stream.")
			return
		}
		if err != nil {
			slog.Error("Error receiving inference data", "err", err)
			return
		}

		eventsingest.TransmitMeasureEvent(time.Now(), "", inferenceData.TaskId, "scheduler3_result_received_compute", map[string]string{})

		slog.Debug("Received inference results", "taskId", inferenceData.TaskId, "processingStatus", inferenceData.ProcessingStatus, "detections", len(inferenceData.Detections))

		if err := clientStream.Send(inferenceData); err != nil {
			slog.Error("Error sending inference data to client", "err", err)
			return
		}

		eventsingest.TransmitMeasureEvent(time.Now(), "", inferenceData.TaskId, "scheduler3_result_sent", map[string]string{})
		slog.Debug("Sent inference data back to client for frame", "taskId", inferenceData.TaskId)
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

	err := eventsingest.Initialize("nats://localhost:4222", "scheduler3")
	if err != nil {
		slog.Warn("Failed to init events ingest client, telemetry will not be tracked", "err", err)
	}
	defer eventsingest.Close()

	go startClientifGRPCServer()

	computeUnitConn, err := grpc.NewClient(*computeAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		slog.Error("Connection to compute failed", "err", err)
	}
	defer computeUnitConn.Close()

	computeStreamClient = pb.NewComputeStreamClient(computeUnitConn)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	slog.Info("\nShutdown signal received. Exiting.")
}
