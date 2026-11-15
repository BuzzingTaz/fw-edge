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

	// Listen for app registrations
	if _, err := o.nc.Subscribe("c2.engine.app_register", o.handleAppRegistration); err != nil {
		return fmt.Errorf("failed to subscribe to app_register topic: %w", err)
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

func (o *Orchestrator) handleAppRegistration(msg *nats.Msg) {
	var req pb.AppRegistrationRequest
	if err := proto.Unmarshal(msg.Data, &req); err != nil {
		slog.Error("Failed to unmarshal app registration request", "err", err)
		return
	}

	o.mu.Lock()
	// Just simple logging/tracking for now
	slog.Info("App component registered", "app_id", req.AppId, "component_id", req.ComponentId, "node_id", req.NodeId)
	o.mu.Unlock()

	// Send ack
	resp := &pb.AppRegistrationResponse{Success: true}
	respData, _ := proto.Marshal(resp)
	if msg.Reply != "" {
		_ = msg.Respond(respData)
	}
}

// DeployApp schedules components to the least loaded nodes and sends START_APP commands
func (o *Orchestrator) DeployApp(req DeployRequest) error {
	o.mu.RLock()
	defer o.mu.RUnlock()

	// Identify active nodes (seen in last 30s)
	activeNodes := make(map[string]*NodeState)
	now := time.Now()
	for id, node := range o.nodes {
		if now.Sub(node.LastSeen) < 30*time.Second {
			activeNodes[id] = node
		}
	}

	if len(activeNodes) == 0 {
		return fmt.Errorf("no active nodes available to deploy app")
	}

	// Schedule each requested component
	for _, compReq := range req.Components {
		for i := 0; i < compReq.Count; i++ {
			// Find least loaded node
			var bestNode string
			var lowestLoad float32 = -1.0

			for id, node := range activeNodes {
				var load float32 = 0.0
				if node.Metrics != nil {
					// Naive load metric: CPU + Memory
					load = node.Metrics.CpuPercent + node.Metrics.MemoryPercent
				}

				if lowestLoad == -1.0 || load < lowestLoad {
					lowestLoad = load
					bestNode = id
				}
			}

			if bestNode == "" {
				// Fallback if no best node found for some reason
				for id := range activeNodes {
					bestNode = id
					break
				}
			}

			slog.Info("Scheduling component", "app_id", req.AppID, "component", compReq.Name, "node_id", bestNode, "load", lowestLoad)

			// Send command
			cmd := &pb.NodeCommand{
				Type:       pb.NodeCommand_START_APP,
				AppId:      req.AppID,
				Components: []string{compReq.Name},
			}
			cmdData, _ := proto.Marshal(cmd)
			topic := fmt.Sprintf("c2.node.%s.command", bestNode)

			if err := o.nc.Publish(topic, cmdData); err != nil {
				slog.Error("Failed to send command to node", "err", err, "node_id", bestNode)
			}
		}
	}

	return nil
}
