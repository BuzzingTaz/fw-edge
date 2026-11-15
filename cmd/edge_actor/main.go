package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"

	pb "github.com/BuzzingTaz/fw-edge-apps/proto"
	"github.com/joho/godotenv"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
)

var (
	natsURL *string
	nodeID  *string
)

func init() {
	godotenv.Load()
	natsURL = flag.String("nats-url", "nats://localhost:4222", "Central Nats server URL for C2")
	nodeID = flag.String("node-id", os.Getenv("NODE_ID"), "Unique ID of this edge node")

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)
}

func main() {
	flag.Parse()

	if *nodeID == "" {
		hostname, _ := os.Hostname()
		if hostname == "" {
			hostname = "unknown-node"
		}
		*nodeID = hostname
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	nc, err := nats.Connect(*natsURL, nats.Name(fmt.Sprintf("Edge Actor %s", *nodeID)), nats.RetryOnFailedConnect(true), nats.MaxReconnects(-1))
	if err != nil {
		slog.Error("Failed to connect to Central NATS", "err", err)
		os.Exit(1)
	}
	defer nc.Close()

	slog.Info("Edge Actor connected to C2", "node_id", *nodeID)

	// Subscribe to commands
	topic := fmt.Sprintf("c2.node.%s.command", *nodeID)
	if _, err := nc.Subscribe(topic, handleCommand); err != nil {
		slog.Error("Failed to subscribe to commands", "err", err)
		os.Exit(1)
	}

	// Register with Central Engine
	registerWithEngine(nc)

	// Status heartbeat
	go statusHeartbeat(ctx, nc)

	<-ctx.Done()
	slog.Info("Shutting down Edge Actor...")
}

func registerWithEngine(nc *nats.Conn) {
	req := &pb.RegisterNodeRequest{
		NodeId:   *nodeID,
		Hostname: *nodeID,
	}
	data, _ := proto.Marshal(req)

	msg, err := nc.Request("c2.engine.register", data, 5*time.Second)
	if err != nil {
		slog.Error("Failed to register with engine", "err", err)
		return
	}

	var resp pb.RegisterNodeResponse
	if err := proto.Unmarshal(msg.Data, &resp); err != nil {
		slog.Error("Failed to unmarshal register response", "err", err)
		return
	}

	if resp.Success {
		slog.Info("Successfully registered with Central Engine", "msg", resp.Message)
	} else {
		slog.Error("Registration rejected", "msg", resp.Message)
	}
}

func statusHeartbeat(ctx context.Context, nc *nats.Conn) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Just send an empty active_apps list for now.
			// Later we can track which apps are actually running by asking docker.
			status := &pb.NodeStatus{
				NodeId: *nodeID,
				// ActiveApps: getActiveApps(),
			}
			data, _ := proto.Marshal(status)
			_ = nc.Publish("c2.engine.status", data)
		case <-ctx.Done():
			return
		}
	}
}

func handleCommand(msg *nats.Msg) {
	var cmd pb.NodeCommand
	if err := proto.Unmarshal(msg.Data, &cmd); err != nil {
		slog.Error("Failed to unmarshal command", "err", err)
		return
	}

	slog.Info("Received command", "type", cmd.Type, "app_id", cmd.AppId)

	switch cmd.Type {
	case pb.NodeCommand_START_APP:
		go func() {
			err := runDockerCompose(cmd.AppId, cmd.Components, "up", "-d")
			if err != nil {
				slog.Error("Failed to start app", "app_id", cmd.AppId, "err", err)
			} else {
				slog.Info("App started successfully", "app_id", cmd.AppId)
			}
		}()
	case pb.NodeCommand_STOP_APP:
		go func() {
			err := runDockerCompose(cmd.AppId, cmd.Components, "down")
			if err != nil {
				slog.Error("Failed to stop app", "app_id", cmd.AppId, "err", err)
			} else {
				slog.Info("App stopped successfully", "app_id", cmd.AppId)
			}
		}()
	}
}

func runDockerCompose(appID string, components []string, args ...string) error {
	// Inside the container, we mounted the full repo to /root/fw-edge
	appDir := fmt.Sprintf("/root/fw-edge/apps/%s", appID)

	composeArgs := append([]string{"compose"}, args...)
	if len(components) > 0 {
		composeArgs = append(composeArgs, components...)
	}

	slog.Info("Running docker compose", "dir", appDir, "cmd", strings.Join(composeArgs, " "))

	c := exec.Command("docker", composeArgs...)
	c.Dir = appDir
	c.Env = os.Environ()
	if hostDir := getHostProjectDir(); hostDir != "" {
		c.Env = append(c.Env, "HOST_PROJECT_DIR="+hostDir)
	}
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr

	if err := c.Run(); err != nil {
		return fmt.Errorf("docker compose failed: %w", err)
	}

	return nil
}

func getHostProjectDir() string {
	if dir := os.Getenv("HOST_PROJECT_DIR"); dir != "" {
		return dir
	}
	out, err := exec.Command("docker", "inspect", "fw-edge-actor", "--format", `{{range .Mounts}}{{if eq .Destination "/root/fw-edge"}}{{.Source}}{{end}}{{end}}`).Output()
	if err == nil {
		if dir := strings.TrimSpace(string(out)); dir != "" {
			return dir
		}
	}
	return ""
}
