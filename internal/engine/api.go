package engine

import (
	"encoding/json"
	"log/slog"
	"net/http"
)

type DeployRequest struct {
	AppID      string         `json:"app_id"`
	Components []ComponentReq `json:"components"`
}

type ComponentReq struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
}

type APIServer struct {
	orch *Orchestrator
}

func NewAPIServer(orch *Orchestrator) *APIServer {
	return &APIServer{orch: orch}
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
		w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func (s *APIServer) Start(addr string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/deploy", s.handleDeploy)

	slog.Info("Starting HTTP API", "addr", addr)
	return http.ListenAndServe(addr, corsMiddleware(mux))
}

func (s *APIServer) handleDeploy(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req DeployRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := s.orch.DeployApp(req); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Deployment started\n"))
}
