package clientif

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	pb "github.com/BuzzingTaz/fw-edge-apps/proto"
	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v4"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client represents a connected user
type Client struct {
	UserID                   string
	Protocol                 string
	ClientConn               *websocket.Conn
	SchedulerGRPCConn        *grpc.ClientConn
	SchedulerStream          grpc.BidiStreamingClient[pb.StreamVideoRequest, pb.InferenceResult]
	PeerConnection           *webrtc.PeerConnection
	DataChannel              *webrtc.DataChannel
	SchedulerListenerHandler func(ProcessedDataMessage)
	Mutex                    sync.Mutex
}

// TODO: Move these to a separate signaling.go file
type WebRTCSignal struct {
	Type string                   `json:"type"` // "offer", "answer", "candidate"
	SDP  string                   `json:"sdp,omitempty"`
	ICE  *webrtc.ICECandidateInit `json:"candidate,omitempty"`
}

type ClientWsMessage struct {
	Signal *WebRTCSignal `json:"webrtc_signal,omitempty"`
}

// temporary
// TODO: ew, change this to grpc protobuf (that's the whole point of grpc, right?)
type ProcessedDataMessage struct {
	Timestamp        uint64 `json:"timestamp"`
	ProcessingStatus int    `json:"processing_status"`
	Detections       []struct {
		X          int     `json:"x"`
		Y          int     `json:"y"`
		Dx         int     `json:"dx"`
		Dy         int     `json:"dy"`
		Label      string  `json:"label"`
		Confidence float64 `json:"confidence"`
	} `json:"detections"`
}

func (client *Client) WriteSignalJSON(v ClientWsMessage) error {
	client.Mutex.Lock()
	defer client.Mutex.Unlock()
	return client.ClientConn.WriteJSON(v)
}

func (client *Client) InitializePC() error {
	var err error
	if client.Protocol != "webrtc" {
		slog.Error("InitiatePC called with unsupported protocol", "protocol", client.Protocol)
		return errors.New("Unsupported protocol: " + client.Protocol)
	}

	config := webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{
			{URLs: []string{"stun:stun.l.google.com:19302"}},
		},
	}

	client.PeerConnection, err = webrtc.NewPeerConnection(config)
	if err != nil {
		return err
	}

	client.PeerConnection.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			return
		}
		candidate := c.ToJSON()
		signal := ClientWsMessage{
			Signal: &WebRTCSignal{
				Type: "candidate",
				ICE:  &candidate,
			},
		}
		if writeErr := client.WriteSignalJSON(signal); writeErr != nil {
			slog.Error("Failed to send ICE candidate", "error", writeErr)
		} else {
			slog.Info("Sent ICE candidate", "userID", client.UserID)
		}
	})

	client.PeerConnection.OnConnectionStateChange(func(s webrtc.PeerConnectionState) {
		slog.Info("PeerConnection State Change", "state", s.String())
	})

	return nil
}

func (client *Client) EstablishConnection() error {
	offer, err := client.PeerConnection.CreateOffer(nil)
	if err != nil {
		return err
	}
	if err = client.PeerConnection.SetLocalDescription(offer); err != nil {
		return err
	}

	// Send Offer via WebSocket
	offerMsg := ClientWsMessage{
		Signal: &WebRTCSignal{
			Type: "offer",
			SDP:  offer.SDP,
		},
	}
	return client.WriteSignalJSON(offerMsg)
}

func (client *Client) ListenWebRTCSignalHandler() {
	var message ClientWsMessage
	for {
		err := client.ClientConn.ReadJSON(&message)
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				slog.Error("WS closed unexpectedly", "err", err)
			}
			break
		}

		if message.Signal != nil {
			slog.Info("Received WebRTC signal", "type", message.Signal.Type, "userID", client.UserID)
			messageSignal := message.Signal

			switch messageSignal.Type {
			case "answer":
				slog.Debug("Received answer from client", "userID", client.UserID)
				answer := webrtc.SessionDescription{
					Type: webrtc.SDPTypeAnswer,
					SDP:  messageSignal.SDP,
				}
				if err = client.PeerConnection.SetRemoteDescription(answer); err != nil {
					slog.Error("SetRemoteDescription failed", "error", err)
				} else {
					slog.Debug("Set remote description with answer", "userID", client.UserID)
				}
			case "candidate":
				slog.Debug("Received ICE candidate from client", "userID", client.UserID)
				if messageSignal.ICE != nil {
					if err = client.PeerConnection.AddICECandidate(*messageSignal.ICE); err != nil {
						slog.Error("AddICECandidate failed", "error", err)
					} else {
						slog.Debug("Added ICE candidate", "userID", client.UserID)
					}
				} else {
					slog.Error("Received nil ICE candidate", "userID", client.UserID)
				}
			default:
				slog.Warn("Unknown message type", "type", messageSignal.Type)
			}
		}

	}
}

func (client *Client) ConnectScheduler() error {
	schedulerConn, err := grpc.NewClient("localhost:5000", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}

	schedulerClient := pb.NewVideoStreamServiceClient(schedulerConn)
	stream, err := schedulerClient.StreamVideo(context.Background())
	if err != nil {
		schedulerConn.Close()
		return err
	}

	client.Mutex.Lock()
	client.SchedulerGRPCConn = schedulerConn
	client.SchedulerStream = stream
	client.Mutex.Unlock()
	slog.Info("Connected to scheduler gRPC", "userID", client.UserID)
	return nil
}

// SendDataToClient sends a generic message to the client via the WebRTC data channel
func SendDataToClient[T any](client *Client, message T) error {
	if client.Protocol != "webrtc" {
		return errors.New("Unsupported protocol: " + client.Protocol)
	}
	if client.PeerConnection == nil {
		return errors.New("PeerConnection not established")
	}
	if client.DataChannel == nil {
		return errors.New("DataChannel not established")
	}
	if client.DataChannel.ReadyState() != webrtc.DataChannelStateOpen {
		return errors.New("DataChannel not open")
	}

	jsonData, err := json.Marshal(message)
	if err != nil {
		return err
	}

	return client.DataChannel.SendText(string(jsonData))
}

func (client *Client) ListenScheduler() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	for {
		result, err := client.SchedulerStream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) || ctx.Err() != nil {
				return
			}
			slog.Error("Failed to read from scheduler", "error", err)
			time.Sleep(1 * time.Second)
			continue
		}

		message := ProcessedDataMessage{
			Timestamp:        result.GetTimestamp(),
			ProcessingStatus: int(result.GetProcessingStatus()),
		}
		for _, detection := range result.GetDetections() {
			message.Detections = append(message.Detections, struct {
				X          int     `json:"x"`
				Y          int     `json:"y"`
				Dx         int     `json:"dx"`
				Dy         int     `json:"dy"`
				Label      string  `json:"label"`
				Confidence float64 `json:"confidence"`
			}{
				X:          int(detection.GetX()),
				Y:          int(detection.GetY()),
				Dx:         int(detection.GetDx()),
				Dy:         int(detection.GetDy()),
				Label:      detection.GetLabel(),
				Confidence: float64(detection.GetConfidence()),
			})
		}

		handler := client.SchedulerListenerHandler
		if handler == nil {
			slog.Warn("No scheduler listener handler set, skipping message", "userID", client.UserID)
			continue
		}
		handler(message)
	}
}
