package main

import (
	"fmt"
	"net/http"

	"github.com/BuzzingTaz/fw-edge-apps/internal/scheduler"
	"github.com/gorilla/websocket"
	"github.com/pion/rtp"
)

type Frame struct {
	Data      []byte `json:"data"`
	Timestamp int64  `json:"timestamp"`
}

var upgrader = websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}

func wsHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("WebSocket handler called")

	userID := r.PathValue("userID")

	if userID == "" {
		http.Error(w, "userID is required", http.StatusBadRequest)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		fmt.Println("WebSocket upgrade failed:", err)
		return
	}
	// defer conn.Close()

	fmt.Println("WebSocket connection established for user:", userID)

	var message rtp.Packet
	go func() {
		for {
			err = conn.ReadJSON(&message)
			if err != nil {
				fmt.Println("WebSocket read error:", err)
				break
			}

			// Handle the message (e.g., WebRTC signaling)
			fmt.Print("Received message at:", message.Timestamp)
			fmt.Println("Data length:", len(message.Payload))
			fmt.Println(" from user:", userID)
			fmt.Println("Data: ", message.Payload)
		}
	}()
}

func main() {
	http.HandleFunc("/ws/{userID}", wsHandler)

	fmt.Println("Server starting on :9998")
	go func() {

		if err := http.ListenAndServe(":9998", nil); err != nil {
			fmt.Println("Failed to start server:", err)
		}
	}()
	fmt.Println("After ListenAndServe")
	// frameConsumer := consumer.NewConsumer(clientID, nats URL, subscribeSubject, queueGroup)
	// go frameConsumer.StartConsuming()

	frameScheduler := scheduler.NewScheduler(nil, nil)
	fmt.Println(frameScheduler)
	// go frameScheduler.Run()

	// wait
	select {}
}
