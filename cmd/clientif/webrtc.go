package main

import (
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/BuzzingTaz/fw-edge-apps/internal/clientif"
	"github.com/BuzzingTaz/fw-edge-apps/internal/eventsingest"
	"github.com/BuzzingTaz/fw-edge-apps/internal/utils"
	pb "github.com/BuzzingTaz/fw-edge-apps/proto"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/rtp/codecs"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media/samplebuilder"
)

func depacketizerForCodec(mimeType string) rtp.Depacketizer {
	switch strings.ToLower(mimeType) {
	case strings.ToLower(webrtc.MimeTypeVP8):
		return &codecs.VP8Packet{}
	case strings.ToLower(webrtc.MimeTypeVP9):
		return &codecs.VP9Packet{}
	case strings.ToLower(webrtc.MimeTypeH264):
		return &codecs.H264Packet{}
	case strings.ToLower(webrtc.MimeTypeH265):
		return &codecs.H265Packet{}
	default:
		return nil
	}
}

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

		codec := track.Codec()
		depacketizer := depacketizerForCodec(codec.MimeType)
		if depacketizer == nil {
			slog.Error("Unsupported video codec", "userID", client.UserID, "mimeType", codec.MimeType)
			return
		}
		sampleBuilder := samplebuilder.New(250, depacketizer, codec.ClockRate)

		// Loop here is fine since it's a separate goroutine
		for {
			rtpPacket, _, readErr := track.ReadRTP()
			if readErr != nil {
				slog.Error("Failed to read RTP packet", "error", readErr)
				break
			}

			sampleBuilder.Push(rtpPacket)

			for sample := sampleBuilder.Pop(); sample != nil; sample = sampleBuilder.Pop() {
				timestamp := uint64(sample.PacketTimestamp)
				taskID, ok := tsToTaskIDMap[timestamp]
				if !ok {
					slog.Debug("Encoded frame with new timestamp received, adding to map", "timestamp", timestamp)
					taskID = uuid.New().String()
					tsToTaskIDMap[timestamp] = taskID
					eventsingest.TransmitMeasureEvent(time.Now(), client.UserID, taskID, "clientif_new_frame_received", map[string]string{
						"timestamp": strconv.FormatUint(timestamp, 10),
					})
				}

				sampleBytes, err := utils.MarshalMediaSample(sample)
				if err != nil {
					slog.Error("Failed to marshal media sample", "error", err)
					continue
				}

				encodedFrame := &pb.EncodedFrame{
					Data:     sampleBytes,
					TaskId:   taskID,
					MimeType: codec.MimeType,
				}
				if err := client.SchedulerStream.Send(encodedFrame); err != nil {
					slog.Error("Failed to send encoded frame to scheduler", "error", err)
					return
				}

				slog.Debug("Sent encoded frame to scheduler", "timestamp", timestamp, "payload_size", len(sampleBytes), "frame_size", len(sample.Data))
			}

			slog.Debug("Read RTP packet:", "rtpPacket", rtpPacket)
			slog.Debug("Received RTP packet", "size", rtpPacket.MarshalSize())
		}
	})

	return nil
}
