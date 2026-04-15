/*
This program is a Go server that receives a video stream (as RTP packets) from one client,
forwards it via UDP, and also receives AI inference results (bounding boxes) from a Python process.
Let me break it down section by section.
*/

package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"sync"

	pbcompute "github.com/BuzzingTaz/fw-edge-apps/cmd/compute/edge-compute-yolo"
	pb "github.com/BuzzingTaz/fw-edge-apps/proto"
	"github.com/pion/rtp"
	"google.golang.org/grpc"
)

// variable to bridge the incoming results from the python to scheduler

var (
	latestBoxes []*pbcompute.BoundingBox
	mu sync.RWMutex
)


var (
	port = flag.Int("port", 9997, "The server port") //func Int(name string, default int, usage string) *int
	// if no port is passed in cli args its takes teh default port as 9997
)

// gRPC server implementation for handling video stream and inference results
type computeStreamServer struct {
	pb.UnimplementedComputeStreamServer // Embedding this struct ensures forward compatibility if new methods are added to the service definition(inheritance)
}

type inferenceServer struct {
	pbcompute.UnimplementedInferenceTrackerServer
}

// StreamResults receives the continuous stream of bounding boxes from Python
func (s *inferenceServer) StreamResults(stream pbcompute.InferenceTracker_StreamResultsServer) error {
	log.Println("Python inference client connected to gRPC stream!")

	for { //infinite loop to receive streaming data until the client disconnects or stream ends or errors.
		res, err := stream.Recv() // blocking call to receive the next inference result from the Python client
		if err == io.EOF {
			log.Println("Python client cleanly closed the gRPC stream.")
			return stream.SendAndClose(&pbcompute.Ack{Received: true})
		}
		if err != nil {
			log.Printf("gRPC stream error (Python likely disconnected): %v", err)
			return err
		}

		mu.Lock()
		latestBoxes = res.Boxes // Update the global variable with the latest bounding boxes received from Python. This allows the scheduler to access the most recent inference results when it needs to send data back to the client.
		mu.Unlock()

		// --- INFERENCE RESULTS ARRIVE HERE ---
		log.Printf("Received %d bounding boxes at timestamp %d", len(res.Boxes), res.Timestamp)

		for _, box := range res.Boxes {
			log.Printf(" - %s (%.2f): [%d, %d, %d, %d]",
				box.ClassLabel, box.Confidence, box.X, box.Y, box.W, box.H)
		}

	}
}


/*
Client sends RTP video packets
Server forwards packets to UDP port
Server sends fake detection results back to client


//TODO: what about teh packet loss and reordering in UDP? Do we need to implement some buffering and sequence checking in the server to handle out-of-order packets? Or do we just assume the network is reliable enough for this demo?
*/

func (*computeStreamServer) StreamVideo(stream grpc.BidiStreamingServer[pb.RTPPacket, pb.InferenceResult]) error {
	packetCount := 0 // just a counter to keep track of how many packets we've received for logging purposes
	conn, err := net.Dial("udp", "127.0.0.1:5000") // (udp client) Establish a UDP connection to the local Python process that will handle the video packets
	if err != nil {
		log.Fatalf("Failed to connect to UDP server: %v", err)
	}
	defer conn.Close()
	log.Println("UDP connection established to localhost:5000")

	for {
		frame, err := stream.Recv() // from the scheduler
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
		packet := &rtp.Packet{} //reference to the pion/rtp library's Packet struct, which has fields like Timestamp, PayloadType, SSRC, SequenceNumber, etc.
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

		conn.Write(frame.Data) // Forward the raw RTP packet bytes to the Python process via UDP

		//TODO: Send back a actual inference result to the client from the python process instead of a mock result. We can have the Python process send the inference results back to this Go server via gRPC, and then we can forward those results to the client in response to each video packet. For now, we'll just send a fake bounding box for demonstration purposes.
		// mockBox := &pb.BoundingBox{
		// 	X:          100,
		// 	Y:          150,
		// 	Dx:      	200,
		// 	Dy:     	400,
		// 	Label:      "Person",
		// 	Confidence: 	0.95,
		// }
//--------------------------------------------------------------------------------
		// get the actual results:
		mu.RLock()
		boxesToSend := latestBoxes
		

		var detections []*pbcompute.BoundingBox
		for _,box := range boxesToSend {
			detections = append(detections, &pbcompute.BoundingBox{
				X:          box.X,
				Y:          box.Y,
				Dx:      	box.Dx,
				Dy:     	box.Dy,
				Label:      box.ClassLabel,
				Confidence: 	box.Confidence,
			})
		}
		mu.RUnlock()

		log.Printf("Sending %d detections back to client for frame timestamp %d\n", len(detections), packet.Timestamp)
//--------------------------------------------------------------------------------
		
		err = stream.Send(&pb.InferenceResult{
			Timestamp: uint64(packet.Timestamp), // Echo the timestamp so the client can sync
			ProcessingStatus:  0,
			// Detections:  []*pb.BoundingBox{mockBox},
			Detections:  detections,
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
	flag.Parse() // getting the cli args for the port number, if any, otherwise it will use the default value of 9997
	go startComputeGRPCServer();

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port)) // starting the gRPC server
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer() // Create a new gRPC server instance
	pb.RegisterComputeStreamServer(s, &computeStreamServer{})


	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil { // inline error handling
		log.Fatalf("failed to serve: %v", err)
	}

}
