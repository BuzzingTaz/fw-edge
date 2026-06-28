package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	pb "github.com/BuzzingTaz/fw-edge-apps/proto"
	"github.com/BuzzingTaz/fw-edge-apps/internal/scheduler"
	"github.com/gorilla/websocket"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

var (
	nodeAddrs = map[string]string{
		"node1": "localhost:9997",
		"node2": "localhost:9995",
		"node3": "localhost:9993",
	}
	globalScheduler *scheduler.Scheduler
	activeStreams   = make(map[string]grpc.BidiStreamingClient[pb.RTPPacket, pb.InferenceResult])
	streamsMu       sync.RWMutex
	upgrader        = websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}

	csvFile *os.File
	logChan = make(chan string, 1000)
)

// FIXED: Single writer goroutine per WebSocket connection
var wsWriteChan chan interface{}
var wsWriteMu sync.RWMutex

// FIXED: Track gRPC connections separately so we can close them
var nodeConns = make(map[string]*grpc.ClientConn)
var connMu sync.RWMutex

func getMetricsURL(addr string) string {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Sprintf("http://%s:9996/metrics", addr)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return fmt.Sprintf("http://%s:9996/metrics", host)
	}
	return fmt.Sprintf("http://%s:%d/metrics", host, port-1)
}

func calculateQoS(m scheduler.NodeMetrics) float64 {
	qosL := math.Exp(-m.Latency / 50.0)

	qosQ := 1.0 - (m.Queue / 5.0)
	if qosQ < 0.0 {
		qosQ = 0.0
	}

	qosA := 1.0
	if m.CPU > 95.0 {
		qosA = 0.0
	}

	qosTH := m.Throughput / 30.0
	if qosTH > 1.0 {
		qosTH = 1.0
	}
	if qosTH < 0.0 {
		qosTH = 0.0
	}

	qosT := (85.0 - m.Temperature) / (85.0 - 45.0)
	if qosT > 1.0 {
		qosT = 1.0
	}
	if qosT < 0.0 {
		qosT = 0.0
	}

	return 0.25*qosL + 0.20*qosQ + 0.20*qosA + 0.20*qosTH + 0.15*qosT
}

func ensureNodeStreams() {
	streamsMu.Lock()
	defer streamsMu.Unlock()

	for nodeName, addr := range nodeAddrs {
		if _, exists := activeStreams[nodeName]; exists && activeStreams[nodeName] != nil {
			continue
		}

		// FIXED: Close old connection if it exists
		connMu.Lock()
		if oldConn, exists := nodeConns[nodeName]; exists && oldConn != nil {
			oldConn.Close()
			delete(nodeConns, nodeName)
		}
		connMu.Unlock()

		// FIXED: Use grpc.Dial with keepalive and blocking
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		conn, err := grpc.DialContext(ctx, addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:                30 * time.Second,
				Timeout:             10 * time.Second,
				PermitWithoutStream: true,
			}),
			grpc.WithBlock(),
		)
		cancel()
		if err != nil {
			slog.Error("Failed to connect to node", "node", nodeName, "addr", addr, "err", err)
			continue
		}

		connMu.Lock()
		nodeConns[nodeName] = conn
		connMu.Unlock()

		client := pb.NewComputeStreamClient(conn)
		stream, err := client.StreamVideo(context.Background())
		if err != nil {
			slog.Error("Failed to open stream to node", "node", nodeName, "addr", addr, "err", err)
			conn.Close()
			continue
		}

		activeStreams[nodeName] = stream
		slog.Info("Established gRPC stream to node", "node", nodeName, "addr", addr)
		go ReadInferenceData(stream, nodeName)
	}
}

func wsHandler(w http.ResponseWriter, r *http.Request) {
	slog.Info("WebSocket handler called")

	pathParts := strings.Split(strings.TrimPrefix(r.URL.Path, "/ws/"), "/")
	userID := pathParts[0]

	if userID == "" {
		http.Error(w, "userID is required", http.StatusBadRequest)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Error("WebSocket upgrade failed", "err", err)
		return
	}

	// FIXED: Create a channel for this connection's writes
	writeChan := make(chan interface{}, 100)
	done := make(chan struct{})

	// FIXED: Single writer goroutine - the ONLY thing that calls WriteJSON
	go func() {
		defer close(done)
		for msg := range writeChan {
			if err := conn.WriteJSON(msg); err != nil {
				slog.Error("WebSocket write error", "err", err)
				return
			}
		}
	}()

	// Store the write channel for ReadInferenceData to use
	wsWriteMu.Lock()
	// Close old connection's write channel if exists
	if oldWriteChan := wsWriteChan; oldWriteChan != nil {
		close(oldWriteChan)
	}
	wsWriteChan = writeChan
	wsWriteMu.Unlock()

	slog.Info("WebSocket connection established for user", "userID", userID)

	ensureNodeStreams()

	// Read frames from frontend
	go func() {
		defer func() {
			conn.Close()
			close(writeChan) // Signal writer to stop
			<-done           // Wait for writer to finish
			slog.Info("WebSocket client disconnected", "userID", userID)
		}()

		for {
			messageType, data, err := conn.ReadMessage()
			if err != nil {
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					slog.Error("WebSocket read error", "err", err)
				}
				return
			}

			if messageType != websocket.BinaryMessage {
				slog.Warn("Received non-binary message, skipping")
				continue
			}

			slog.Debug("Received JPEG frame", "size", len(data))

			frame := &scheduler.Frame{
				Data:      data,
				Timestamp: time.Now().UnixMilli(),
			}
			globalScheduler.ScheduleFrame(frame)
		}
	}()
}

func ReadInferenceData(stream grpc.BidiStreamingClient[pb.RTPPacket, pb.InferenceResult], nodeName string) {
	for {
		inferenceData, err := stream.Recv()
		if err == io.EOF {
			slog.Info("Server closed the inference stream.", "node", nodeName)
			streamsMu.Lock()
			delete(activeStreams, nodeName)
			streamsMu.Unlock()
			return
		}
		if err != nil {
			slog.Error("Error receiving inference data", "node", nodeName, "err", err)
			streamsMu.Lock()
			delete(activeStreams, nodeName)
			streamsMu.Unlock()
			return
		}

		slog.Info("Received inference results",
			"node", nodeName,
			"timestamp", inferenceData.Timestamp,
			"detections", len(inferenceData.Detections))

		// FIXED: Send to channel instead of direct WriteJSON
		wsWriteMu.RLock()
		ch := wsWriteChan
		wsWriteMu.RUnlock()

		if ch != nil {
			select {
			case ch <- inferenceData:
			default:
				slog.Warn("WebSocket write channel full, dropping result")
			}
		} else {
			slog.Warn("No WebSocket write channel available")
		}
	}
}

func init() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)
}

func main() {
	flag.Parse()

	nodes := []string{"node1", "node2", "node3"}
	globalScheduler = scheduler.NewScheduler(nodes, "http://localhost:5010")

	var err error
	csvFile, err = os.OpenFile("scheduler_metrics.csv", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		slog.Error("Failed to open metrics CSV file", "err", err)
	} else {
		csvFile.WriteString("timestamp,cpu,memory,temperature,queue,latency,throughput,power,selected_policy,selected_node,qos,dqn_inference_ms,scheduler_decision_ms,total_frames,node1_frames,node2_frames,node3_frames,policy0_frames,policy1_frames,policy2_frames,policy3_frames\n")
		go func() {
			for logLine := range logChan {
				if csvFile != nil {
					csvFile.WriteString(logLine)
				}
			}
		}()
	}

	globalScheduler.OnDispatch = func(node scheduler.Node, f *scheduler.Frame) {
    m, exists := globalScheduler.GetNodeMetrics(string(node))
    if exists && m.Queue > 15 {
        slog.Warn("Node queue full, dropping frame", "node", node, "queue", m.Queue)
        return
    }

    // FIXED: Retry loop with reconnection
    maxRetries := 3
    for attempt := 0; attempt < maxRetries; attempt++ {
        streamsMu.RLock()
        stream, exists := activeStreams[string(node)]
        streamsMu.RUnlock()

        if !exists || stream == nil {
            slog.Warn("No active stream, attempting reconnect", "node", string(node), "attempt", attempt+1)
            ensureNodeStreams()
            time.Sleep(100 * time.Millisecond)
            continue
        }

        err := stream.Send(&pb.RTPPacket{Data: f.Data})
        if err == nil {
            return // Success
        }

        slog.Error("Failed to dispatch frame, removing stream", "node", string(node), "err", err, "attempt", attempt+1)
        streamsMu.Lock()
        delete(activeStreams, string(node))
        streamsMu.Unlock()
        
        // Force reconnection on next attempt
        ensureNodeStreams()
        time.Sleep(200 * time.Millisecond)
    }
    
    slog.Error("Failed to dispatch frame after retries, dropping", "node", string(node))
  }

	go func() {
		client := &http.Client{Timeout: 200 * time.Millisecond}
		for {
			for _, node := range nodes {
				addr := nodeAddrs[node]
				url := getMetricsURL(addr)
				resp, err := client.Get(url)
				if err != nil {
					continue
				}
				var m scheduler.NodeMetrics
				if err := json.NewDecoder(resp.Body).Decode(&m); err == nil {
					globalScheduler.UpdateNodeMetrics(node, m)
				}
				resp.Body.Close()
			}
			time.Sleep(500 * time.Millisecond)
		}
	}()

	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			if globalScheduler == nil {
				continue
			}

			globalScheduler.StatsMu.RLock()
			total := globalScheduler.TotalFrames
			n1 := globalScheduler.NodeFrames["node1"]
			n2 := globalScheduler.NodeFrames["node2"]
			n3 := globalScheduler.NodeFrames["node3"]
			p0 := globalScheduler.PolicyFrames[0]
			p1 := globalScheduler.PolicyFrames[1]
			p2 := globalScheduler.PolicyFrames[2]
			p3 := globalScheduler.PolicyFrames[3]
			dqnDur := globalScheduler.DqnInferenceTime
			decDur := globalScheduler.DecisionTime
			selNode := globalScheduler.LastSelectedNode
			selPol := globalScheduler.LastSelectedPolicy
			globalScheduler.StatsMu.RUnlock()

			if selNode == "" {
				selNode = "node1"
			}

			m, exists := globalScheduler.GetNodeMetrics(selNode)
			if !exists {
				continue
			}

			qos := calculateQoS(m)
			ts := time.Now().Unix()

			slog.Info("Metrics Telemetry",
				"time", ts,
				"node", selNode,
				"policy", selPol,
				"cpu", fmt.Sprintf("%.1f%%", m.CPU),
				"mem", fmt.Sprintf("%.1f%%", m.Memory),
				"temp", fmt.Sprintf("%.1fC", m.Temperature),
				"queue", m.Queue,
				"lat", fmt.Sprintf("%.1fms", m.Latency),
				"tp", fmt.Sprintf("%.1ffps", m.Throughput),
				"power", fmt.Sprintf("%.2fW", m.Power),
				"qos", fmt.Sprintf("%.3f", qos),
				"dqn_ms", fmt.Sprintf("%.2fms", dqnDur),
				"decision_ms", fmt.Sprintf("%.2fms", decDur),
				"total_frames", total,
			)

			logLine := fmt.Sprintf("%d,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%d,%s,%.4f,%.2f,%.2f,%d,%d,%d,%d,%d,%d,%d,%d\n",
				ts, m.CPU, m.Memory, m.Temperature, m.Queue, m.Latency, m.Throughput, m.Power,
				selPol, selNode, qos, dqnDur, decDur, total, n1, n2, n3, p0, p1, p2, p3)

			select {
			case logChan <- logLine:
			default:
			}
		}
	}()

	http.HandleFunc("/ws/", wsHandler)

	slog.Info("Server starting on :9998")
	go func() {
		if err := http.ListenAndServe(":9998", nil); err != nil {
			slog.Error("Failed to start server", "err", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	slog.Info("Shutdown signal received. Cleaning up...")
	close(logChan)
	if csvFile != nil {
		csvFile.Close()
	}
	streamsMu.Lock()
	for name := range activeStreams {
		delete(activeStreams, name)
	}
	streamsMu.Unlock()
	connMu.Lock()
	for name, conn := range nodeConns {
		if conn != nil {
			conn.Close()
		}
		delete(nodeConns, name)
	}
	connMu.Unlock()
	slog.Info("Exiting.")
}
