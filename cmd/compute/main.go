package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"strings"

	"github.com/BuzzingTaz/fw-edge-apps/internal/utils"
	pb "github.com/BuzzingTaz/fw-edge-apps/proto"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

var (
	port = flag.Int("port", 9997, "The server port")
)

const udsSocketPath = "/tmp/edge_compute_inference.sock"

type computeStreamServer struct {
	pb.UnimplementedComputeStreamServer
}

const (
	codecUnknown uint8 = 0
	codecVP8     uint8 = 1
	codecH264    uint8 = 2
	codecVP9     uint8 = 3
	codecH265    uint8 = 4
)

func codecIDFromMimeType(mimeType string) uint8 {
	switch {
	case strings.Contains(strings.ToLower(mimeType), "vp8"):
		return codecVP8
	case strings.Contains(strings.ToLower(mimeType), "h264"):
		return codecH264
	case strings.Contains(strings.ToLower(mimeType), "vp9"):
		return codecVP9
	case strings.Contains(strings.ToLower(mimeType), "h265"), strings.Contains(strings.ToLower(mimeType), "hevc"):
		return codecH265
	default:
		return codecUnknown
	}
}

// sendEncodedFrameToUDS writes dynamic length-prefixed variables to the Unix socket
func sendEncodedFrameToUDS(conn net.Conn, taskID string, codecID uint8, encodedData []byte) error {
	taskIDBytes := []byte(taskID)
	taskIDLen := len(taskIDBytes)
	dataLen := len(encodedData)

	// Allocate buffer: 2 (taskID len) + taskID strings + 1 (codec) + 4 (data len) + frame bytes
	buf := make([]byte, 2+taskIDLen+1+4+dataLen)

	binary.BigEndian.PutUint16(buf[0:2], uint16(taskIDLen))
	copy(buf[2:2+taskIDLen], taskIDBytes)

	buf[2+taskIDLen] = codecID

	binary.BigEndian.PutUint32(buf[3+taskIDLen:7+taskIDLen], uint32(dataLen))
	copy(buf[7+taskIDLen:], encodedData)

	_, err := conn.Write(buf)
	return err
}

func (*computeStreamServer) StreamEncodedFrames(stream grpc.BidiStreamingServer[pb.EncodedFrame, pb.InferenceResult]) error {
	conn, err := net.Dial("unix", udsSocketPath)
	if err != nil {
		log.Printf("Failed to connect to Python UDS server at %s: %v", udsSocketPath, err)
		return err
	}
	defer conn.Close()
	log.Println("UDS connection established to Python inference engine.")

	errChan := make(chan error, 2)

	// Goroutine 1: Read incoming inferences from Python UDS and forward to gRPC client
	go func() {
		lenBuf := make([]byte, 4)
		for {
			// Read 4-byte length prefix
			if _, err := io.ReadFull(conn, lenBuf); err != nil {
				if err != io.EOF {
					log.Printf("Error reading length from UDS: %v", err)
				}
				errChan <- err
				return
			}
			msgLen := binary.BigEndian.Uint32(lenBuf)

			// Read Protobuf payload
			msgBuf := make([]byte, msgLen)
			if _, err := io.ReadFull(conn, msgBuf); err != nil {
				log.Printf("Error reading payload from UDS: %v", err)
				errChan <- err
				return
			}

			var result pb.InferenceResult
			if err := proto.Unmarshal(msgBuf, &result); err != nil {
				log.Printf("Error unmarshaling InferenceResult from UDS: %v", err)
				continue
			}

			if err := stream.Send(&result); err != nil {
				log.Printf("Error sending inference data to gRPC client: %v", err)
				errChan <- err
				return
			}
		}
	}()

	// Goroutine 2: Receive encoded frames from gRPC client and send to Python UDS
	go func() {
		for {
			encodedFrame, err := stream.Recv()
			if err == io.EOF {
				log.Println("Client finished sending encoded frames.")
				errChan <- nil
				return
			}
			if err != nil {
				log.Printf("Error receiving encoded frame: %v", err)
				errChan <- err
				return
			}

			sample, err := utils.UnmarshalMediaSample(encodedFrame.GetData())
			if err != nil {
				log.Printf("Error unmarshaling MediaSample from EncodedFrame: %v", err)
				continue
			}

			// log.Printf("EncodedFrame received, packet_timestamp=%d taskID=%s mime_type=%s frame_size=%d",
			// 	sample.PacketTimestamp, encodedFrame.GetTaskId(), encodedFrame.GetMimeType(), len(sample.Data))

			err = sendEncodedFrameToUDS(
				conn,
				encodedFrame.GetTaskId(),
				codecIDFromMimeType(encodedFrame.GetMimeType()),
				sample.Data)

			if err != nil {
				log.Printf("Error forwarding encoded frame to Python: %v", err)
				errChan <- err
				return
			}
		}
	}()

	return <-errChan
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

	go startSchedulerGRPCServer()

	select {}
}
