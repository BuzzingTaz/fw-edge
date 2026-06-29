package scheduler

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/keepalive"
)

type Nodes []string
type Node string

type NodeMetrics struct {
	CPU         float64 `json:"cpu"`
	Memory      float64 `json:"memory"`
	Temperature float64 `json:"temperature"`
	Queue       float64 `json:"queue"`
	Latency     float64 `json:"latency"`
	Throughput  float64 `json:"throughput"`
	Power       float64 `json:"power"`
}

type Scheduler struct {
	nodes              Nodes
	metricsMu          sync.RWMutex
	nodeMetrics        map[string]NodeMetrics
	dqnSidecarURL      string
	rrIndex            int
	rrMutex            sync.Mutex
	OnDispatch         func(Node, *Frame)
	// Telemetry statistics
	StatsMu            sync.RWMutex
	TotalFrames        int64
	NodeFrames         map[string]int64
	PolicyFrames       map[int]int64
	DqnInferenceTime   float64 // in ms
	DecisionTime       float64 // in ms
	LastSelectedNode   string
	LastSelectedPolicy int
	// Epsilon-greedy exploration
	epsilon            float64
	epsilonDecay       float64
	epsilonMin         float64
	// gRPC connection pool with keepalive
	nodeConns          map[string]*grpc.ClientConn
	connMu             sync.RWMutex
}

func NewScheduler(nodes Nodes, dqnURL string) *Scheduler {
	s := &Scheduler{
		nodes:              nodes,
		nodeMetrics:        make(map[string]NodeMetrics),
		dqnSidecarURL:      dqnURL,
		rrIndex:            0,
		NodeFrames:         make(map[string]int64),
		PolicyFrames:       make(map[int]int64),
		epsilon:            0.30,  // Start with 30% exploration
		epsilonDecay:       0.995, // Decay per frame
		epsilonMin:         0.05,  // Minimum 5% exploration
		nodeConns:          make(map[string]*grpc.ClientConn),
	}
	// Initialize default metrics for nodes
	for _, node := range nodes {
		s.nodeMetrics[node] = NodeMetrics{
			CPU:         20.0,
			Memory:      20.0,
			Temperature: 35.0,
			Queue:       0.0,
			Latency:     5.0,
			Throughput:  30.0,
			Power:       5.0,
		}
		s.NodeFrames[node] = 0
	}
	s.PolicyFrames[0] = 0
	s.PolicyFrames[1] = 0
	s.PolicyFrames[2] = 0
	return s
}

// GetNodeConnection returns a gRPC connection to a node with proper keepalive settings
func (s *Scheduler) GetNodeConnection(nodeAddr string) (*grpc.ClientConn, error) {
	s.connMu.RLock()
	conn, exists := s.nodeConns[nodeAddr]
	s.connMu.RUnlock()

	if exists && conn != nil {
		state := conn.GetState()
		// FIXED: Use connectivity package instead of grpc.Connecting/grpc.Ready
		if state != connectivity.Shutdown && state != connectivity.TransientFailure {
			return conn, nil
		}
		// Connection is dead, close it
		conn.Close()
	}

	// Create new connection with keepalive
	s.connMu.Lock()
	defer s.connMu.Unlock()

	// Double-check after acquiring write lock
	if conn, exists := s.nodeConns[nodeAddr]; exists && conn != nil {
		state := conn.GetState()
		if state != connectivity.Shutdown && state != connectivity.TransientFailure {
			return conn, nil
		}
		conn.Close()
	}

	kaParams := keepalive.ClientParameters{
		Time:                30 * time.Second,
		Timeout:             10 * time.Second,
		PermitWithoutStream: true,
	}

	conn, err := grpc.Dial(nodeAddr,
		grpc.WithInsecure(),
		grpc.WithKeepaliveParams(kaParams),
		grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"round_robin"}`),
	)
	if err != nil {
		return nil, err
	}

	s.nodeConns[nodeAddr] = conn
	return conn, nil
}

// CloseAllConnections closes all managed connections
func (s *Scheduler) CloseAllConnections() {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	for addr, conn := range s.nodeConns {
		if conn != nil {
			conn.Close()
		}
		delete(s.nodeConns, addr)
	}
}

// UpdateNodeMetrics allows updating system state parameters for a node
func (s *Scheduler) UpdateNodeMetrics(node string, metrics NodeMetrics) {
	s.metricsMu.Lock()
	defer s.metricsMu.Unlock()
	s.nodeMetrics[node] = metrics
}

// GetNodeMetrics retrieves a copy of metrics for a node (thread-safe)
func (s *Scheduler) GetNodeMetrics(node string) (NodeMetrics, bool) {
	s.metricsMu.RLock()
	defer s.metricsMu.RUnlock()
	m, exists := s.nodeMetrics[node]
	return m, exists
}

// ConstructStateVector builds the 18-dimensional state vector for 3 nodes
func (s *Scheduler) ConstructStateVector() []float64 {
	s.metricsMu.RLock()
	defer s.metricsMu.RUnlock()

	state := make([]float64, 0, 18)
	for i := 0; i < 3; i++ {
		nodeName := "node1"
		if i < len(s.nodes) {
			nodeName = s.nodes[i]
		}
		m := s.nodeMetrics[nodeName]
		state = append(state, m.CPU, m.Memory, m.Temperature, m.Queue, m.Latency, m.Throughput)
	}
	return state
}

// QueryDQNSidecar calls the Python HTTP server to select an action policy.
func (s *Scheduler) QueryDQNSidecar(state []float64) (int, []float64, error) {
	payload := map[string][]float64{"state": state}
	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		return 0, nil, err
	}

	client := &http.Client{Timeout: 100 * time.Millisecond}
	resp, err := client.Post(s.dqnSidecarURL+"/predict", "application/json", bytes.NewBuffer(jsonPayload))
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()

	var result struct {
		Action   int       `json:"action"`
		QValues  []float64 `json:"q_values"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, nil, err
	}
	return result.Action, result.QValues, nil
}

// calculateNodeScore returns a score where HIGHER is better
func (s *Scheduler) calculateNodeScore(node string, m NodeMetrics) float64 {
	// Score components (all normalized to 0-1 range, higher = better)

	// Latency score: lower latency = higher score
	latencyScore := math.Exp(-m.Latency / 100.0)

	// Queue score: lower queue = higher score
	queueScore := 1.0 - (m.Queue / 20.0)
	if queueScore < 0 {
		queueScore = 0
	}

	// Throughput score: higher throughput = higher score
	throughputScore := m.Throughput / 30.0
	if throughputScore > 1.0 {
		throughputScore = 1.0
	}

	// CPU score: lower CPU = higher score (but not zero until >95%)
	cpuScore := 1.0
	if m.CPU > 95.0 {
		cpuScore = 0
	} else if m.CPU > 80.0 {
		cpuScore = 1.0 - (m.CPU-80.0)/15.0
	}

	// Temperature score: lower temp = higher score
	tempScore := (85.0 - m.Temperature) / (85.0 - 45.0)
	if tempScore > 1.0 {
		tempScore = 1.0
	}
	if tempScore < 0 {
		tempScore = 0
	}

	// Memory score: lower memory = higher score
	memScore := 1.0 - (m.Memory / 100.0)
	if memScore < 0 {
		memScore = 0
	}

	// Weighted combination
	score := 0.25*latencyScore + 0.25*queueScore + 0.20*throughputScore +
	          0.10*cpuScore + 0.10*tempScore + 0.10*memScore

	return score
}

// SelectNodeHierarchical executes the DQN selected policy to select a worker node
func (s *Scheduler) SelectNodeHierarchical(frame *Frame) (Node, int) {
	startDecision := time.Now()

	state := s.ConstructStateVector()

	// Try DQN first
	startDQN := time.Now()
	action, qValues, err := s.QueryDQNSidecar(state)
	dqnDur := time.Since(startDQN).Seconds() * 1000.0

	if err != nil {
		fmt.Printf("DQN query failed: %v, using heuristic\n", err)
		action = -1 // Force heuristic
		dqnDur = 0
	}

	// Epsilon-greedy: override DQN with random exploration
	s.StatsMu.Lock()
	currentEpsilon := s.epsilon
	// Decay epsilon
	s.epsilon = s.epsilon * s.epsilonDecay
	if s.epsilon < s.epsilonMin {
		s.epsilon = s.epsilonMin
	}
	s.StatsMu.Unlock()

	if rand.Float64() < currentEpsilon {
		action = rand.Intn(3)
		fmt.Printf("EXPLORATION: epsilon=%.3f, random action=%d\n", currentEpsilon, action)
	} else {
		fmt.Printf("EXPLOITATION: DQN action=%d, q_values=%v\n", action, qValues)
	}

	var pickedNode string

	// Get current metrics for all nodes
	s.metricsMu.RLock()
	metrics := make(map[string]NodeMetrics)
	for _, node := range s.nodes {
		metrics[node] = s.nodeMetrics[node]
	}
	s.metricsMu.RUnlock()

	switch action {
	case 0:
		// Action 0: Closest Node (Node 0) - but check if it's overloaded
		pickedNode = s.nodes[0]
		if metrics[pickedNode].Queue > 15 {
			// Fallback to best available node
			pickedNode = s.findBestNode(metrics)
			action = 3 // Mark as fallback
		}

	case 1:
		// Action 1: Load Balancing (Round-Robin) - with queue check
		s.rrMutex.Lock()
		// Find next node with queue <= 15
		for i := 0; i < len(s.nodes); i++ {
			candidate := s.nodes[s.rrIndex]
			s.rrIndex = (s.rrIndex + 1) % len(s.nodes)
			if metrics[candidate].Queue <= 15 {
				pickedNode = candidate
				break
			}
		}
		// If all nodes overloaded, pick least loaded
		if pickedNode == "" {
			pickedNode = s.findBestNode(metrics)
			action = 3
		}
		s.rrMutex.Unlock()

	case 2:
		// Action 2: Least Impedance - fixed to use score-based selection
		pickedNode = s.findBestNode(metrics)

	default:
		// Fallback: pick best node
		pickedNode = s.findBestNode(metrics)
		action = 3
	}

	// Final safety check: if selected node queue > 20, force best node
	if metrics[pickedNode].Queue > 20 {
		fmt.Printf("WARNING: Node %s queue=%.1f too high, forcing best node\n", pickedNode, metrics[pickedNode].Queue)
		pickedNode = s.findBestNode(metrics)
		action = 3
	}

	decisionDur := time.Since(startDecision).Seconds() * 1000.0

	// Update stats
	s.StatsMu.Lock()
	s.TotalFrames++
	s.NodeFrames[pickedNode]++
	s.PolicyFrames[action]++
	s.DqnInferenceTime = dqnDur
	s.DecisionTime = decisionDur
	s.LastSelectedNode = pickedNode
	s.LastSelectedPolicy = action
	s.StatsMu.Unlock()

	return Node(pickedNode), action
}

// findBestNode returns the node with the highest score (lowest impedance)
func (s *Scheduler) findBestNode(metrics map[string]NodeMetrics) string {
	bestNode := s.nodes[0]
	bestScore := -1.0

	for _, node := range s.nodes {
		score := s.calculateNodeScore(node, metrics[node])
		fmt.Printf("  Node %s score=%.3f (queue=%.1f, lat=%.1f, tp=%.1f)\n",
			node, score, metrics[node].Queue, metrics[node].Latency, metrics[node].Throughput)
		if score > bestScore {
			bestScore = score
			bestNode = node
		}
	}

	fmt.Printf("  -> Best node: %s (score=%.3f)\n", bestNode, bestScore)
	return bestNode
}

// ScheduleFrame schedules and dispatches the frame using hierarchical policy
func (s *Scheduler) ScheduleFrame(frame *Frame) {
	fmt.Println("********** SCHEDULEFRAME CALLED **********", frame.Timestamp)
	pickedNode, action := s.SelectNodeHierarchical(frame)
	fmt.Printf("Scheduler: Action=%d Selected Node=%s for Frame Timestamp=%d\n", action, string(pickedNode), frame.Timestamp)
	if s.OnDispatch != nil {
		s.OnDispatch(pickedNode, frame)
	}
}
