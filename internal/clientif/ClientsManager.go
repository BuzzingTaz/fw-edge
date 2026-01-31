package clientif

import (
	"sync"
	"log/slog"
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

func (cm *ClientsManager) CreateClient(userID string) *Client {
	cm.ClientsMu.Lock()
	defer cm.ClientsMu.Unlock()

	client := &Client{
		UserID: userID,
	}
	cm.Clients[userID] = client
	slog.Info("Client created", "userID", userID)
	return client
}

func (cm *ClientsManager) AddClient(client *Client) {
	cm.ClientsMu.Lock()
	defer cm.ClientsMu.Unlock()

	// TODO: Check for existing client
	cm.Clients[client.UserID] = client
	slog.Info("Client added", "userID", client.UserID)
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
		slog.Info("Closed client connection", "userID", userID)
	}
}

