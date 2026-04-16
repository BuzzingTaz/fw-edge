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

		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
		defer cancel()
		// NOTE: Only creating rtp streaming for now, change to decoded frames (gRPC)
		//sending to compute server (gRPC)
		computeVideoStreamer, err := computeStreamClient.StreamVideo(ctx)
		if err != nil {
			log.Println("Failed to create rpc from client: %v", err)
			return
		}

		go ReadInferenceData(computeVideoStreamer) // in another thread

		for {
			// recieve video frames from client via websocket, marshal them into rtp packets and send to compute server via gRPC stream
			err = tempConn.ReadJSON(&message)
			if err != nil {
				fmt.Println("WebSocket read error:", err)
				break
			}

			fmt.Println("Received message at:", message.Timestamp)
			fmt.Println("Data length:", len(message.Payload))
			fmt.Println(" from user:", userID)

			// convert rtp to raw bytes to send to compute server via gRPC
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

// read the inference result from the compute server and forward it to the client via the websocket connection. This will run in a separate goroutine so that it can listen for inference results while still allowing the main goroutine to listen for incoming video frames from the client.
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
		log.Printf("Received processed data for frame timestamp  %d: %d (%d detections)",
			inferenceData.Timestamp, inferenceData.ProcessingStatus, len(inferenceData.Detections))

		if tempConn != nil {
			//write back to client via websocket
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

	//Connect to Compute Server (gRPC)
	computeUnitConn, err := grpc.NewClient(*computeAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect to compute: %v", err)
	}
	defer computeUnitConn.Close()
	computeStreamClient = pb.NewComputeStreamClient(computeUnitConn)

	// starting the Web Socket server to send inference results back to the client in a seperate thread.
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) { fmt.Println("Bello!") })
	http.HandleFunc("/ws/{userID}", wsHandler) //when client connects to ws, it will call the wsHandler function which will upgrade the connection to a websocket and start listening for messages from the client (e.g., frames to process) and also send inference results back to the client when they are received from the compute server. The userID in the path can be used to identify different clients if needed.
	fmt.Println("Server starting on :9998")
	
	go func() {
		if err := http.ListenAndServe(":9998", nil); err != nil {
			fmt.Println("Failed to start server:", err)
		}
	}()

	// frameConsumer := consumer.NewConsumer(clientID, nats URL, subscribeSubject, queueGroup)
	// go frameConsumer.StartConsuming()
	
	//TODO: Initialize the scheduler with the appropriate parameters (e.g., list of compute nodes, scheduling algorithm, etc.)
	frameScheduler := scheduler.NewScheduler(nil, nil, []string{"node1", "node2", "node3"})
	fmt.Println(frameScheduler)
	// go frameScheduler.Run()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	fmt.Println("\nShutdown signal received. Exiting.")
}
