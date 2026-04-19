package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	pbcompute "github.com/BuzzingTaz/fw-edge-apps/cmd/compute/edge-compute-yolo"
	pb "github.com/BuzzingTaz/fw-edge-apps/proto"
	"github.com/pion/rtp"
	"google.golang.org/grpc"
)

var (
	port = flag.Int("port", 9997, "The server port")
)

type InferenceCache struct {
	mu    sync.RWMutex
	Boxes []*pb.BoundingBox
}

var sharedInferenceCache = &InferenceCache{}

type inferenceServer struct {
	pbcompute.UnimplementedInferenceTrackerServer
}

// StreamResults receives the continuous stream of bounding boxes from Python
func (s *inferenceServer) StreamResults(stream pbcompute.InferenceTracker_StreamResultsServer) error {
	log.Println("Python inference client connected to gRPC stream!")

	for {
		res, err := stream.Recv()
		if err == io.EOF {
			log.Println("Python client cleanly closed the gRPC stream.")
			return stream.SendAndClose(&pbcompute.Ack{Received: true})
		}
		if err != nil {
			log.Printf("gRPC stream error (Python likely disconnected): %v", err)
			return err
		}

		var newDetections []*pb.BoundingBox
		log.Printf("Received %d bounding boxes at timestamp %d", len(res.Boxes), res.Timestamp)

		for _, box := range res.Boxes {
			newDetections = append(newDetections, &pb.BoundingBox{
				X:          uint64(box.X),
				Y:          uint64(box.Y),
				Dx:         uint64(box.W),
				Dy:         uint64(box.H),
				Label:      box.ClassLabel,
				Confidence: box.Confidence,
			})
		}

		sharedInferenceCache.mu.Lock()
		sharedInferenceCache.Boxes = newDetections
		sharedInferenceCache.mu.Unlock()
	}
}

type computeStreamServer struct {
	pb.UnimplementedComputeStreamServer
}

func (*computeStreamServer) StreamVideo(stream grpc.BidiStreamingServer[pb.RTPPacket, pb.InferenceResult]) error {
	conn, err := net.Dial("udp", "127.0.0.1:5000")
	if err != nil {
		log.Fatalf("Failed to connect to UDP server: %v", err)
	}
	defer conn.Close()
	log.Println("UDP connection established to localhost:5000")

	errChan := make(chan error, 2)

	packetCount := 0
	var latestRTPTimestamp uint64
	var tsMutex sync.RWMutex

	go func() {
		for {
			frame, err := stream.Recv()
			if err == io.EOF {
				// Stream closed by the client
				log.Println("Client finished sending frames.")
				errChan <- nil
				return
			}
			if err != nil {
				log.Printf("Error receiving frame: %v", err)
				errChan <- err
				return
			}

			// 2. Unmarshal the raw bytes back into an RTP Packet
			packet := &rtp.Packet{}
			if err := packet.Unmarshal(frame.Data); err == nil {
				tsMutex.Lock()
				latestRTPTimestamp = uint64(packet.Timestamp)
				packetCount++
				tsMutex.Unlock()
			}
			log.Printf("packet received with timestamp %v", packet.Timestamp)

			conn.Write(frame.Data)
		}
	}()

	go func() {

		ticker := time.NewTicker(33 * time.Millisecond) // Send results back at 30/s
		defer ticker.Stop()
		for {
			select {
			case <-stream.Context().Done():
				// If the parent context cancels (client disconnects), kill this loop
				errChan <- stream.Context().Err()
				return
			case <-ticker.C:

				// Could have race condition if cache is being updated faster than ticker
				sharedInferenceCache.mu.Lock()
				currentBoxes := sharedInferenceCache.Boxes
				sharedInferenceCache.Boxes = nil
				sharedInferenceCache.mu.Unlock()

				if currentBoxes == nil {
					continue
				}

				tsMutex.RLock()
				ts := latestRTPTimestamp
				tsMutex.RUnlock()

				err := stream.Send(&pb.InferenceResult{
					Timestamp:        ts,
					ProcessingStatus: 0,
					Detections:       currentBoxes,
				})

				if err != nil {
					log.Printf("Error sending inference data to client: %v", err)
					errChan <- err
					return
				}
			}
		}
	}()

	return <-errChan
}

func startComputeGRPCServer() {
	lis, err := net.Listen("tcp", ":5005")
	if err != nil {
		log.Fatalf("Failed to listen on gRPC port 5005: %v", err)
	}

	s := grpc.NewServer()
	pbcompute.RegisterInferenceTrackerServer(s, &inferenceServer{})

	log.Println("Go compute gRPC server listening on :5005")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve gRPC: %v", err)
	}
}

func startSchedulerGRPCServer() {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterComputeStreamServer(s, &computeStreamServer{})

	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func main() {
	flag.Parse()

	go startComputeGRPCServer()
	go startSchedulerGRPCServer()

	select {}
}
