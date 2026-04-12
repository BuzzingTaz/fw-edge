package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"

	pb "github.com/BuzzingTaz/fw-edge-apps/proto"
	pbcompute "github.com/BuzzingTaz/fw-edge-apps/cmd/compute/edge-compute-yolo"
	"github.com/pion/rtp"
	"google.golang.org/grpc"
)

var (
	port = flag.Int("port", 9997, "The server port")
)

type computeStreamServer struct {
	pb.UnimplementedComputeStreamServer
}

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

		// --- INFERENCE RESULTS ARRIVE HERE ---
		log.Printf("Received %d bounding boxes at timestamp %d", len(res.Boxes), res.Timestamp)

		for _, box := range res.Boxes {
			log.Printf(" - %s (%.2f): [%d, %d, %d, %d]",
				box.ClassLabel, box.Confidence, box.X, box.Y, box.W, box.H)
		}

	}
}

func (*computeStreamServer) StreamVideo(stream grpc.BidiStreamingServer[pb.RTPPacket, pb.InferenceResult]) error {
	packetCount := 0
	conn, err := net.Dial("udp", "127.0.0.1:5000")
	if err != nil {
		log.Fatalf("Failed to connect to UDP server: %v", err)
	}
	defer conn.Close()
	log.Println("UDP connection established to localhost:5000")

	for {
		frame, err := stream.Recv()
		if err == io.EOF {
			// Stream closed by the client
			log.Println("Client finished sending frames.")
			return nil
		}
		if err != nil {
			log.Printf("Error receiving frame: %v", err)
			return err
		}


		// 2. Unmarshal the raw bytes back into an RTP Packet
		packet := &rtp.Packet{}
		err = packet.Unmarshal(frame.Data)
		if err != nil {
			log.Println("Error unmarshaling RTP packet:", err)
			continue
		}

		// 3. Process your video packet here!
		// e.g., write to a file, pass to a media server, etc.
		packetCount++
		// log.Printf("packet received with timestamp %v", packet.Timestamp)
		log.Printf("packet received with timestamp %v, Payload Type: %d", packet.Timestamp, packet.PayloadType)
		// log.Printf("Received packet from SSRC %d, Sequence: %d\n", packet.SSRC, packet.SequenceNumber)

		conn.Write(frame.Data)

		mockBox := &pb.BoundingBox{
			X:          100,
			Y:          150,
			Dx:      	200,
			Dy:     	400,
			Label:      "Person",
			Confidence: 	0.95,
		}

		err = stream.Send(&pb.InferenceResult{
			Timestamp: uint64(packet.Timestamp), // Echo the timestamp so the client can sync
			ProcessingStatus:  0,
			Detections:  []*pb.BoundingBox{mockBox},
		})

		if err != nil {
			log.Printf("Error sending inference data back to client: %v", err)
			return err
		}
	}
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

func main() {
	flag.Parse()
	go startComputeGRPCServer();

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
