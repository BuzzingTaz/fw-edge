package main

import (
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/BuzzingTaz/fw-edge-apps/internal/clientif"
	"github.com/BuzzingTaz/fw-edge-apps/internal/metrics"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v4"
)

func HandleWebRTC(client *clientif.Client, w http.ResponseWriter, r *http.Request) {
	var upgrader = websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	clientConn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Error("WebSocket upgrade failed", "error", err)
		return
	}
	client.ClientConn = clientConn
	slog.Info("Signalling WebSocket connection established for ", "userID", client.UserID)

	go client.ListenWebRTCSignalHandler()

	if err := client.InitializePC(); err != nil {
		slog.Error("Failed to establish PeerConnection", "error", err)
		return
	}
	slog.Info("WebRTC connection Initialized for ", "userID", client.UserID)

	if err := InitializeDataChannel(client); err != nil {
		slog.Error("Failed to create data channel", "error", err)
		return
	}

	if err := InitializeVideoTransceiver(client); err != nil {
		slog.Error("Failed to set OnTrack handler", "error", err)
		return
	}

	if err := client.EstablishConnection(); err != nil {
		slog.Error("Failed to establish WebRTC connection", "error", err)
		return
	}
	slog.Info("WebRTC connection established for ", "userID", client.UserID)
}

func InitializeDataChannel(client *clientif.Client) error {
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
		slog.Debug("Data channel message received", "userID", client.UserID, "message", string(msg.Data))
	})

	return nil
}

func InitializeVideoTransceiver(client *clientif.Client) error {
	if _, err := client.PeerConnection.AddTransceiverFromKind(
		webrtc.RTPCodecTypeVideo,
		webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly},
	); err != nil {
		return err
	}

	client.PeerConnection.OnTrack(func(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
		slog.Info("Received remote track", "userID", client.UserID, "trackID", track.ID(), "trackKind", track.Kind().String())

		// Ticker to send PLIs every 5 seconds to request keyframes from the client
		go func() {
			ticker := time.NewTicker(time.Second * 5)
			defer ticker.Stop()
			for range ticker.C {
				err := client.PeerConnection.WriteRTCP([]rtcp.Packet{
					&rtcp.PictureLossIndication{
						MediaSSRC: uint32(track.SSRC()),
					},
				})
				if err != nil {
					slog.Warn("Failed to send PLI, stopping", "error", err)
					return
				}
			}
		}()

		// Loop here is fine since it's a separate goroutine
		for {
			rtpPacket, _, readErr := track.ReadRTP()
			if readErr != nil {
				slog.Error("Failed to read RTP packet", "error", readErr)
				break
			}
			taskID, ok := tsToTaskIDMap[uint64(rtpPacket.Timestamp)]
			if !ok {
				slog.Debug("RTP packet with New Timestamp received, adding to map", "timestamp", rtpPacket.Timestamp)
				taskID = uuid.New().String()
				tsToTaskIDMap[uint64(rtpPacket.Timestamp)] = taskID
				metrics.SampleEvent(time.Now(), client.UserID, taskID, "clientif_new_rtp_received", map[string]string{
					"timestamp": strconv.FormatUint(uint64(rtpPacket.Timestamp), 10),
				})
			}

			if rtpPacket.Marker {
				slog.Debug("RTP packet with Marker bit found", "timestamp", rtpPacket.Timestamp)
				metrics.SampleEvent(time.Now(), client.UserID, taskID, "clientif_marker_rtp_received", map[string]string{
					"timestamp": strconv.FormatUint(uint64(rtpPacket.Timestamp), 10),
				})
			}

			client.SchedulerConn.WriteJSON(rtpPacket)

			slog.Debug("Read RTP packet:", rtpPacket)
			slog.Info("Received RTP packet", "size", rtpPacket.MarshalSize())
		}
	})

	return nil
}
