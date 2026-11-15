package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/BuzzingTaz/fw-edge-apps/internal/metrics"
)

const benchmarksDir = "/benchmarks"

func (s *EdgeIngestServer) setupHTTPServer() {
	mux := http.NewServeMux()

	withCORS := func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
			if r.Method == "OPTIONS" {
				w.WriteHeader(http.StatusOK)
				return
			}
			next(w, r)
		}
	}

	mux.HandleFunc("/api/benchmarks/edge/save", withCORS(s.handleSaveTelemetry))
	mux.HandleFunc("/api/benchmarks/client/save", withCORS(s.handleSaveClientTelemetry))
	mux.HandleFunc("/api/benchmarks/list", withCORS(s.handleListBenchmarks))
	mux.HandleFunc("/api/benchmarks/{userId}/files", withCORS(s.handleListBenchmarkFiles))
	mux.HandleFunc("/api/benchmarks/{userId}/download/{filename}", withCORS(s.handleDownloadBenchmarkFile))
	mux.HandleFunc("/api/benchmarks/clear", withCORS(s.handleClearTelemetry))

	go func() {
		slog.Info("Starting benchmark HTTP server", "addr", ":8888", "dir", benchmarksDir)
		if err := http.ListenAndServe(":8888", mux); err != nil {
			slog.Error("HTTP server failed", "error", err)
		}
	}()
}

func (s *EdgeIngestServer) handleSaveTelemetry(w http.ResponseWriter, r *http.Request) {
	var req struct {
		UserID string `json:"userId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}
	if !validBenchmarkName(req.UserID) {
		http.Error(w, "Invalid userId", http.StatusBadRequest)
		return
	}

	s.flushPending(r.Context())

	dirpath := filepath.Join(benchmarksDir, req.UserID)
	if err := os.MkdirAll(dirpath, 0755); err != nil {
		slog.Error("Failed to create benchmark directory", "err", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	filename := fmt.Sprintf("edge_telemetry_%s.csv", req.UserID)
	outPath := filepath.Join(dirpath, filename)

	file, err := os.Create(outPath)
	if err != nil {
		slog.Error("Failed to create CSV file", "err", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	if err := s.store.DumpEventsCSV(r.Context(), req.UserID, writer.Write); err != nil {
		slog.Error("Failed to dump events", "err", err)
		http.Error(w, "Failed to dump events", http.StatusInternalServerError)
		return
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		slog.Error("Failed to flush CSV", "err", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	processedPath := filepath.Join(dirpath, fmt.Sprintf("%s_processed.csv", req.UserID))
	if err := metrics.ProcessEdgeCSV(outPath, processedPath); err != nil {
		slog.Error("Failed to process edge telemetry", "err", err, "runId", req.UserID)
	} else {
		slog.Info("Processed edge telemetry", "runId", req.UserID, "path", processedPath)
	}

	slog.Info("Saved edge telemetry", "filename", filename, "userID", req.UserID)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"filename": filename})
}

func (s *EdgeIngestServer) handleSaveClientTelemetry(w http.ResponseWriter, r *http.Request) {
	var req struct {
		UserID string `json:"userId"`
		CSV    string `json:"csv"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || req.CSV == "" {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}
	if !validBenchmarkName(req.UserID) {
		http.Error(w, "Invalid userId", http.StatusBadRequest)
		return
	}

	dirpath := filepath.Join(benchmarksDir, req.UserID)
	if err := os.MkdirAll(dirpath, 0755); err != nil {
		slog.Error("Failed to create benchmark directory", "err", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	filename := fmt.Sprintf("client_telemetry_%s.csv", req.UserID)
	outPath := filepath.Join(dirpath, filename)
	if err := os.WriteFile(outPath, []byte(req.CSV), 0644); err != nil {
		slog.Error("Failed to write client CSV", "err", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	slog.Info("Saved client telemetry", "filename", filename, "userID", req.UserID)
	w.WriteHeader(http.StatusOK)
}

func (s *EdgeIngestServer) handleListBenchmarks(w http.ResponseWriter, r *http.Request) {
	files, err := os.ReadDir(benchmarksDir)
	if err != nil && !os.IsNotExist(err) {
		slog.Error("Failed to read benchmarks directory", "err", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	dirs := []string{}
	for _, file := range files {
		if file.IsDir() {
			dirs = append(dirs, file.Name())
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(dirs)
}

func (s *EdgeIngestServer) handleListBenchmarkFiles(w http.ResponseWriter, r *http.Request) {
	userId := r.PathValue("userId")
	if !validBenchmarkName(userId) {
		http.Error(w, "Invalid userId", http.StatusBadRequest)
		return
	}

	dirpath := filepath.Join(benchmarksDir, userId)
	files, err := os.ReadDir(dirpath)
	if err != nil {
		if os.IsNotExist(err) {
			http.Error(w, "Benchmark not found", http.StatusNotFound)
			return
		}
		slog.Error("Failed to read benchmark directory", "err", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	filenames := []string{}
	for _, file := range files {
		if !file.IsDir() {
			filenames = append(filenames, file.Name())
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(filenames)
}

func (s *EdgeIngestServer) handleDownloadBenchmarkFile(w http.ResponseWriter, r *http.Request) {
	userId := r.PathValue("userId")
	filename := r.PathValue("filename")
	if !validBenchmarkName(userId) || !validBenchmarkName(filename) {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	outPath := filepath.Join(benchmarksDir, userId, filename)
	if _, err := os.Stat(outPath); os.IsNotExist(err) {
		http.Error(w, "File not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Access-Control-Expose-Headers", "Content-Disposition")
	http.ServeFile(w, r, outPath)
}

func (s *EdgeIngestServer) handleClearTelemetry(w http.ResponseWriter, r *http.Request) {
	if err := s.store.ClearEvents(r.Context()); err != nil {
		slog.Error("Failed to clear events", "err", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
	slog.Info("Measurement events cleared")
	w.WriteHeader(http.StatusOK)
}

func validBenchmarkName(name string) bool {
	return name != "" && !strings.Contains(name, "/") && !strings.Contains(name, "\\") && name != "." && name != ".."
}
