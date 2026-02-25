package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"

	pb "github.com/BuzzingTaz/fw-edge-apps/proto"
	"github.com/pion/rtp"
	"google.golang.org/grpc"
)

var (
	port = flag.Int("port", 9997, "The server port")
)

type computeStreamServer struct {
	pb.UnimplementedComputeStreamServer
}

func (*computeStreamServer) StreamVideo(stream grpc.BidiStreamingServer[pb.RTPPacket, pb.InferenceResult]) error {
	packetCount := 0

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
		log.Printf("packet received with timestamp %v", packet.Timestamp)
		// log.Printf("Received packet from SSRC %d, Sequence: %d\n", packet.SSRC, packet.SequenceNumber)

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

func main() {
	flag.Parse()
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
