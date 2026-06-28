package clientif

import (
	"errors"
	"log/slog"
	"sync"
)

type ClientsManager struct {
	Clients   map[string]*Client
	ClientsMu sync.RWMutex
}

func NewClientsManager() *ClientsManager {
	return &ClientsManager{
		Clients: make(map[string]*Client),
	}
}

func (cm *ClientsManager) CreateClient(userID string, protocol string) (*Client, error) {
	cm.ClientsMu.Lock()
	defer cm.ClientsMu.Unlock()

	if !IsValidProtocol(protocol) {
		return nil, errors.New("invalid protocol")
	}

	if _, exists := cm.Clients[userID]; exists {
		slog.Warn("Client already exists, overwriting", "userID", userID)
		// return nil, errors.New("Client already exists")
	}

	client := &Client{
		UserID: userID,
		Protocol: protocol,
	}
	cm.Clients[userID] = client
	slog.Info("Client created", "userID", userID)
	return client, nil
}


func (cm *ClientsManager) RemoveClient(userID string) {
	cm.ClientsMu.Lock()
	defer cm.ClientsMu.Unlock()

	// TODO: Better cleanup (close connections, etc.)
	client := cm.Clients[userID]

	err := client.ClientConn.Close()
	if err != nil {
		slog.Error("Failed to close WebSocket", "error", err)
	}
	err = client.PeerConnection.Close()
	if err != nil {
		slog.Error("Failed to close PeerConnection", "error", err)
	}

	if client.SchedulerStream != nil {
		client.SchedulerStream.CloseSend()
	}
	if client.SchedulerGRPCConn != nil {
		err = client.SchedulerGRPCConn.Close()
		if err != nil {
			slog.Error("Failed to close scheduler gRPC connection", "error", err)
		}
	}

	delete(cm.Clients, userID)
	slog.Info("Client removed", "userID", userID)
}

func (cm *ClientsManager) CloseAll() {
	for userID := range cm.Clients {
		cm.RemoveClient(userID)
	}
}

