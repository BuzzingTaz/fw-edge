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
		slog.Error("Client already exists, overwriting", "userID", userID)
		return nil, errors.New("Client already exists")
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
	err := cm.Clients[userID].ClientConn.Close()
	if err != nil {
		slog.Error("Failed to close WebSocket", "error", err)
	}
	err = cm.Clients[userID].PeerConnection.Close()
	if err != nil {
		slog.Error("Failed to close PeerConnection", "error", err)
	}

	err = cm.Clients[userID].SchedulerConn.Close()
	if err != nil {
		slog.Error("Failed to close Scheduler WebSocket", "error", err)
	}

	delete(cm.Clients, userID)
	slog.Info("Client removed", "userID", userID)
}

func (cm *ClientsManager) CloseAll() {
	for userID := range cm.Clients {
		cm.RemoveClient(userID)
	}
}

