package clientif

import (
	"log/slog"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v4"
)

// Client represents a connected user
type Client struct {
	UserID         string
	ClientConn     *websocket.Conn
	SchedulerConn  *websocket.Conn
	PeerConnection *webrtc.PeerConnection
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

func (client *Client) WriteSignalJSON(v ClientWsMessage) error {
	client.Mutex.Lock()
	defer client.Mutex.Unlock()
	return client.ClientConn.WriteJSON(v)
}

func (client *Client) EstablishPC() error {
	var err error

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

	dataChannel.OnOpen(func() {
		slog.Info("Data channel opened", "userID", client.UserID)
	})
	dataChannel.OnMessage(func(msg webrtc.DataChannelMessage) {
		slog.Info("Data channel message received", "userID", client.UserID, "message", string(msg.Data))
	})

	client.PeerConnection.OnTrack(func(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
		slog.Info("Track received", "kind", track.Kind().String(), "id", track.ID())

		// Actual processing and streaming happens on a separate goroutine
		go func() {
			for {
				rtpPacket, _, readErr := track.ReadRTP()
				if readErr != nil {
					slog.Error("Failed to read RTP packet", "error", readErr)
				}

				slog.Info("Read RTP packet:", rtpPacket)
				client.SchedulerConn.WriteJSON(rtpPacket)

				// Print size of received packet
				slog.Info("Received RTP packet", "size", rtpPacket.MarshalSize())

				slog.Info("RtpPacket Payload:", "payload", rtpPacket.Payload)
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

func (client *Client) ConnectScheduler() error {
	schedulerConn, _, err := websocket.DefaultDialer.Dial("ws://localhost:9999/ws/"+client.UserID, nil)
	if err != nil {
		return err
	}
	client.SchedulerConn = schedulerConn
	slog.Info("Connected to scheduler WebSocket", "userID", client.UserID)
	return nil
}
