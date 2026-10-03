package engine

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	pb "github.com/BuzzingTaz/fw-edge-apps/proto"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
)

type NodeState struct {
	NodeID       string
	Hostname     string
	LastSeen     time.Time
	ActiveApps   []string
	Metrics      *pb.ResourceMetrics
}

type Orchestrator struct {
	mu    sync.RWMutex
	nodes map[string]*NodeState
	apps  map[string][]string // AppID -> NodeIDs running it
	nc    *nats.Conn
}

func NewOrchestrator(nc *nats.Conn) *Orchestrator {
	return &Orchestrator{
		nodes: make(map[string]*NodeState),
		apps:  make(map[string][]string),
		nc:    nc,
	}
}

// Start sets up subscriptions to listen for node registrations and status updates
func (o *Orchestrator) Start(ctx context.Context) error {
	// Listen for registrations
	if _, err := o.nc.Subscribe("c2.engine.register", o.handleRegister); err != nil {
		return fmt.Errorf("failed to subscribe to register topic: %w", err)
	}

	// Listen for node status
	if _, err := o.nc.Subscribe("c2.engine.status", o.handleStatus); err != nil {
		return fmt.Errorf("failed to subscribe to status topic: %w", err)
	}

	// Listen for metrics
	if _, err := o.nc.Subscribe("c2.engine.metrics", o.handleMetrics); err != nil {
		return fmt.Errorf("failed to subscribe to metrics topic: %w", err)
	}

	slog.Info("Orchestrator started, listening for node events on C2")
	return nil
}

func (o *Orchestrator) handleRegister(msg *nats.Msg) {
	var req pb.RegisterNodeRequest
	if err := proto.Unmarshal(msg.Data, &req); err != nil {
		slog.Error("Failed to unmarshal registration request", "err", err)
		return
	}

	o.mu.Lock()
	o.nodes[req.NodeId] = &NodeState{
		NodeID:   req.NodeId,
		Hostname: req.Hostname,
		LastSeen: time.Now(),
	}
	o.mu.Unlock()

	slog.Info("Node registered", "node_id", req.NodeId, "hostname", req.Hostname)

	// Send ack
	resp := &pb.RegisterNodeResponse{
		Success: true,
		Message: "Registered successfully",
	}
	respData, _ := proto.Marshal(resp)
	if msg.Reply != "" {
		_ = msg.Respond(respData)
	}
}

func (o *Orchestrator) handleStatus(msg *nats.Msg) {
	var status pb.NodeStatus
	if err := proto.Unmarshal(msg.Data, &status); err != nil {
		slog.Error("Failed to unmarshal status update", "err", err)
		return
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	if node, exists := o.nodes[status.NodeId]; exists {
		node.LastSeen = time.Now()
		node.ActiveApps = status.ActiveApps
	} else {
		slog.Warn("Received status from unknown node", "node_id", status.NodeId)
	}
}

func (o *Orchestrator) handleMetrics(msg *nats.Msg) {
	var metrics pb.ResourceMetrics
	if err := proto.Unmarshal(msg.Data, &metrics); err != nil {
		slog.Error("Failed to unmarshal metrics", "err", err)
		return
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	if node, exists := o.nodes[metrics.NodeId]; exists {
		node.Metrics = &metrics
		node.LastSeen = time.Now()
	}
}

// DeployApp selects a node (simplified for now: picks the first one or round robin) and sends a START_APP command
func (o *Orchestrator) DeployApp(appID string) error {
	o.mu.RLock()
	// very naive scheduling for now: pick a random active node
	var selectedNode string
	for nodeID := range o.nodes {
		selectedNode = nodeID
		break
	}
	o.mu.RUnlock()

	if selectedNode == "" {
		return fmt.Errorf("no active nodes available to deploy app")
	}

	cmd := &pb.NodeCommand{
		Type:  pb.NodeCommand_START_APP,
		AppId: appID,
	}
	cmdData, _ := proto.Marshal(cmd)

	topic := fmt.Sprintf("c2.node.%s.command", selectedNode)
	if err := o.nc.Publish(topic, cmdData); err != nil {
		return fmt.Errorf("failed to send command to node: %w", err)
	}

	slog.Info("Sent deploy command to node", "app_id", appID, "node_id", selectedNode)
	return nil
}
