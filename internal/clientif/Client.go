package clientif

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v4"
)

// Client represents a connected user
type Client struct {
	UserID         string
	Protocol       string
	ClientConn     *websocket.Conn
	SchedulerConn  *websocket.Conn
	PeerConnection *webrtc.PeerConnection
	DataChannel    *webrtc.DataChannel
	Mutex          sync.Mutex
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

func (client *Client) InitiatePC() error {
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
	if _, err = client.PeerConnection.AddTransceiverFromKind(
		webrtc.RTPCodecTypeVideo,
		webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly},
	); err != nil {
		return err
	}

	// Add Data channel
	dataChannel, err := client.PeerConnection.CreateDataChannel("data", nil)
	if err != nil {
		return err
	}

	client.DataChannel = dataChannel

	dataChannel.OnOpen(func() {
		slog.Info("Data channel opened", "userID", client.UserID)
	})
	dataChannel.OnMessage(func(msg webrtc.DataChannelMessage) {
		slog.Info("Data channel message received", "userID", client.UserID, "message", string(msg.Data))
	})

	client.PeerConnection.OnTrack(func(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
		slog.Info("Track received", "kind", track.Kind().String(), "id", track.ID())

		// Ticker to send PLIs every 2 seconds to request keyframes from the client
		go func() {
			ticker := time.NewTicker(time.Second * 2)
			defer ticker.Stop()
			for range ticker.C {
				err := client.PeerConnection.WriteRTCP([]rtcp.Packet{
					&rtcp.PictureLossIndication{
						MediaSSRC: uint32(track.SSRC()),
					},
				})
				if err != nil {
					// If the connection closes, exit the loop
					return
				}
			}
		}()
		// Actual processing and streaming happens on a separate goroutine
		go func() {
			for {
				rtpPacket, _, readErr := track.ReadRTP()
				if readErr != nil {
					slog.Error("Failed to read RTP packet", "error", readErr)
					break
				}

				// slog.Info("Read RTP packet:", rtpPacket)
				client.SchedulerConn.WriteJSON(rtpPacket)

				// Print size of received packet
				slog.Info("Received RTP packet", "size", rtpPacket.MarshalSize())

				slog.Info("RtpPacket Payload Header:", "Header", rtpPacket.Header)
			}
		}()
	})

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
				slog.Error("WS closed unexpectedly: ", err)
			}
			break
		}

		if message.Signal != nil {
			slog.Info("Received WebRTC signal", "type", message.Signal.Type)
			messageSignal := message.Signal

			switch messageSignal.Type {
			case "answer":
				slog.Info("Received answer from client", "userID", client.UserID)
				answer := webrtc.SessionDescription{
					Type: webrtc.SDPTypeAnswer,
					SDP:  messageSignal.SDP,
				}
				if err = client.PeerConnection.SetRemoteDescription(answer); err != nil {
					slog.Error("SetRemoteDescription failed", "error", err)
				} else {
					slog.Info("Set remote description with answer", "userID", client.UserID)
				}
			case "candidate":
				slog.Info("Received ICE candidate from client", "userID", client.UserID)
				if messageSignal.ICE != nil {
					if err = client.PeerConnection.AddICECandidate(*messageSignal.ICE); err != nil {
						slog.Error("AddICECandidate failed", "error", err)
					} else {
						slog.Info("Added ICE candidate", "userID", client.UserID)
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
	schedulerConn, _, err := websocket.DefaultDialer.Dial("ws://localhost:9998/ws/"+client.UserID, nil)
	if err != nil {
		return err
	}
	client.Mutex.Lock()
	client.SchedulerConn = schedulerConn
	client.Mutex.Unlock()
	slog.Info("Connected to scheduler WebSocket", "userID", client.UserID)
	return nil
}

// SendDataToPeer sends a generic message to the client via the WebRTC data channel
func SendDataToPeer[T any](client *Client, message T) error {
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

func ListenScheduler[T any](client *Client, callback func(*Client, T)) {
	var message T
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		err := client.SchedulerConn.ReadJSON(&message)
		if err != nil {
			slog.Error("Failed to read from scheduler", "error", err)
			break
		}

		callback(client, message)
	}
}

