package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/BuzzingTaz/fw-edge-apps/internal/scheduler"
	pb "github.com/BuzzingTaz/fw-edge-apps/proto"
	"github.com/gorilla/websocket"
	"github.com/pion/rtp"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const httpServerPort = 9998

var computeAddr = flag.String("compute-addr", "localhost:9997", "address to connect to")

var computeStreamClient pb.ComputeStreamClient

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func wsHandler(w http.ResponseWriter, r *http.Request) {

	userID := r.PathValue("userID")

	if userID == "" {
		http.Error(
			w,
			"userID required",
			http.StatusBadRequest,
		)
		return
	}

	// LOCAL per-client websocket.
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("upgrade failed:", err)
		return
	}
	defer conn.Close()

	log.Println("client connected:", userID)

	// Long-lived stream, tied to connection lifetime.
	ctx, cancel := context.WithCancel(
		context.Background(),
	)
	defer cancel()

	// PER-CLIENT gRPC stream
	stream, err := computeStreamClient.StreamVideo(ctx)
	if err != nil {
		log.Println("grpc stream failed:", err)
		return
	}
	defer stream.CloseSend()

	// goroutine reads inference for THIS client only
	go ReadInferenceData(conn, stream)

	// read RTP packets from websocket
	for {
		var pkt rtp.Packet

		err := conn.ReadJSON(&pkt)
		if err != nil {
			log.Println(
				"client disconnected:",
				userID,
				err,
			)
			return
		}

		rawBytes, err := pkt.Marshal()
		if err != nil {
			log.Println("marshal error:", err)
			continue
		}

		err = stream.Send(
			&pb.RTPPacket{
				Data: rawBytes,
			},
		)

		if err != nil {
			log.Println(
				"grpc send error:",
				err,
			)
			return
		}
	}
}

func ReadInferenceData(
	conn *websocket.Conn,
	stream grpc.BidiStreamingClient[
		pb.RTPPacket,
		pb.InferenceResult,
	],
) {

	for {

		result, err := stream.Recv()
		if err == io.EOF {
			log.Println("compute closed stream")
			return
		}

		if err != nil {
			log.Println("inference recv error:", err)
			return
		}

		log.Printf(
			"frame=%d detections=%d",
			result.Timestamp,
			len(result.Detections),
		)

		// write back ONLY to this client's websocket
		err = conn.WriteJSON(result)
		if err != nil {
			log.Println(
				"ws write error:",
				err,
			)
			return
		}
	}
}

func main() {

	flag.Parse()

	//Connect to Compute Server (gRPC)
	computeConn, err := grpc.NewClient(*computeAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		log.Fatalf(
			"compute connect failed: %v",
			err,
		)
	}

	defer computeConn.Close()

	computeStreamClient =
		pb.NewComputeStreamClient(
			computeConn,
		)

	http.HandleFunc("/", func(
		w http.ResponseWriter,
		r *http.Request,
	) {
		fmt.Println("Bello!")
	})

	http.HandleFunc("/ws/{userID}", wsHandler)

	go func() {
		log.Printf("server on :%d", httpServerPort)

		if err := http.ListenAndServe(":9998", nil); err != nil {
			log.Fatal(err)
		}
	}()

	fmt.Println("Number of goroutines running: ", runtime.NumGoroutine())

	frameScheduler := scheduler.NewScheduler(nil, nil)

	fmt.Println(frameScheduler)

	stop := make(chan os.Signal, 1)

	signal.Notify(
		stop,
		syscall.SIGINT,
		syscall.SIGTERM,
	)

	<-stop

	fmt.Println("shutdown")
}
