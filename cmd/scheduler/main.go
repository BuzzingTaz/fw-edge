package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/BuzzingTaz/fw-edge-apps/internal/scheduler"
	pb "github.com/BuzzingTaz/fw-edge-apps/proto"
	"github.com/gorilla/websocket"
	"github.com/pion/rtp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	frameX = 1280
	frameY = 720
)

const httpServerPort = 9998

var computeAddr = flag.String("compute-addr", "localhost:9997", "the address to connect to")
var computeStreamClient pb.ComputeStreamClient

var tempConn *websocket.Conn
var upgrader = websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}

func wsHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	fmt.Println("WebSocket handler called")

	userID := r.PathValue("userID")

	if userID == "" {
		http.Error(w, "userID is required", http.StatusBadRequest)
		return
	}

	tempConn, err = upgrader.Upgrade(w, r, nil)
	if err != nil {
		fmt.Println("WebSocket upgrade failed:", err)
		return
	}
	// defer conn.Close()
	log.Println("WebSocket connection established for user:", userID)

	// go SendFrameData(...)
	go func() {
		var message rtp.Packet

		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		// NOTE: Only creating rtp streaming for now, change to decoded frames
		computeVideoStreamer, err := computeStreamClient.StreamVideo(ctx)
		if err != nil {
			log.Println("Failed to create rpc from client: %v", err)
			return
		}

		go ReadInferenceData(computeVideoStreamer)

		for {
			err = tempConn.ReadJSON(&message)
			if err != nil {
				fmt.Println("WebSocket read error:", err)
				break
			}

			fmt.Println("Received message at:", message.Timestamp)
			fmt.Println("Data length:", len(message.Payload))
			fmt.Println(" from user:", userID)

			rawBytes, err := message.Marshal()
			if err != nil {
				log.Println("Error marshaling RTP packet:", err)
				continue
			}
			// FIXME: SEND ACTUAL FRAMES LATER! This is just marshalled rtp packets
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
			log.Println("Server closed the inference stream.")
			return
		}
		if err != nil {
			log.Printf("Error receiving inference data: %v", err)
			return
		}

		// Process your inference data here!
		log.Printf("Received inference for frame %d: %d (%d detections)",
			inferenceData.Timestamp, inferenceData.ProcessingStatus, len(inferenceData.Detections))

		if tempConn != nil {
			err = tempConn.WriteJSON(inferenceData)
			if err != nil {
				log.Printf("Error sending inference data to client: %v", err)
				return
			}
			log.Println("Sent inference data back to client for frame", inferenceData.Timestamp)
		} else {
			log.Println("No WebSocket connection to send inference data to client.")
		}

	}
}

func main() {
	flag.Parse()

	ffmpeg := exec.Command("ffmpeg", "-i", "pipe:0", "-pix_fmt", "bgr24", "-s", strconv.Itoa(frameX)+"x"+strconv.Itoa(frameY), "-f", "rawvideo", "pipe:1") //nolint
	// ffmpegIn, _ := ffmpeg.StdinPipe()
	// ffmpegOut, _ := ffmpeg.StdoutPipe()
	ffmpegErr, _ := ffmpeg.StderrPipe()

	if err := ffmpeg.Start(); err != nil {
		panic(err)
	}
	go func() {
		scanner := bufio.NewScanner(ffmpegErr)
		for scanner.Scan() {
			fmt.Println(scanner.Text())
		}
	}()

	computeUnitConn, err := grpc.NewClient(*computeAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect to compute: %v", err)
	}
	defer computeUnitConn.Close()
	computeStreamClient = pb.NewComputeStreamClient(computeUnitConn)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) { fmt.Println("Bello!") })
	http.HandleFunc("/ws/{userID}", wsHandler)
	fmt.Println("Server starting on :9998")
	go func() {
		if err := http.ListenAndServe(":9998", nil); err != nil {
			fmt.Println("Failed to start server:", err)
		}
	}()

	// frameConsumer := consumer.NewConsumer(clientID, nats URL, subscribeSubject, queueGroup)
	// go frameConsumer.StartConsuming()

	frameScheduler := scheduler.NewScheduler(nil, nil, []string{"node1", "node2", "node3"})
	fmt.Println(frameScheduler)
	// go frameScheduler.Run()

	// wait
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM) // Wait for SIGINT or SIGTERM signals
	<-stop                                               // Block until a signal is received

	fmt.Println("\nShutdown signal received. Exiting.")
}
