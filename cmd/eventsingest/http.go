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
)

func setupHTTPServer() {
	mux := http.NewServeMux()

	// CORS middleware wrapper
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

	mux.HandleFunc("/api/benchmarks/edge/save", withCORS(handleSaveTelemetry))
	mux.HandleFunc("/api/benchmarks/client/save", withCORS(handleSaveClientTelemetry))
	mux.HandleFunc("/api/benchmarks/list", withCORS(handleListBenchmarks))
	mux.HandleFunc("/api/benchmarks/{userId}/files", withCORS(handleListBenchmarkFiles))
	mux.HandleFunc("/api/benchmarks/{userId}/download/{filename}", withCORS(handleDownloadBenchmarkFile))
	mux.HandleFunc("/api/benchmarks/clear", withCORS(handleClearTelemetry)) // Keep clear for db

	go func() {
		slog.Info("Starting HTTP server on :8888")
		if err := http.ListenAndServe(":8888", mux); err != nil {
			slog.Error("HTTP Server failed", "error", err)
		}
	}()
}

func handleSaveTelemetry(w http.ResponseWriter, r *http.Request) {
	var req struct {
		UserID string `json:"userId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}

	dirpath := filepath.Join("/benchmarks", req.UserID)
	if err := os.MkdirAll(dirpath, 0755); err != nil {
		slog.Error("Failed to create user benchmark directory", "err", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	filename := fmt.Sprintf("edge_telemetry_%s.csv", req.UserID)
	filepath := filepath.Join(dirpath, filename)

	file, err := os.Create(filepath)
	if err != nil {
		slog.Error("Failed to create CSV file", "err", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	err = serverState.db.DumpEventsCSV(r.Context(), req.UserID, writer.Write)
	if err != nil {
		slog.Error("Failed to dump events", "err", err)
		http.Error(w, "Failed to dump events", http.StatusInternalServerError)
		return
	}

	slog.Info("Saved edge telemetry to disk", "filename", filename, "userID", req.UserID)

	// Trigger metrics processing
	go func() {
		metricsURL := "http://metrics:8889/api/process/edge"
		payload := fmt.Sprintf(`{"runId":"%s"}`, req.UserID)
		resp, err := http.Post(metricsURL, "application/json", strings.NewReader(payload))
		if err != nil {
			slog.Error("Failed to trigger edge metrics processing", "err", err, "runId", req.UserID)
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			slog.Error("Metrics processing returned non-OK status", "status", resp.Status, "runId", req.UserID)
		} else {
			slog.Info("Successfully triggered edge metrics processing", "runId", req.UserID)
		}
	}()

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"filename": filename})
}

func handleSaveClientTelemetry(w http.ResponseWriter, r *http.Request) {
	var req struct {
		UserID string `json:"userId"`
		CSV    string `json:"csv"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || req.CSV == "" {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}

	dirpath := filepath.Join("/benchmarks", req.UserID)
	if err := os.MkdirAll(dirpath, 0755); err != nil {
		slog.Error("Failed to create user benchmark directory", "err", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	filename := fmt.Sprintf("client_telemetry_%s.csv", req.UserID)
	filepath := filepath.Join(dirpath, filename)

	err := os.WriteFile(filepath, []byte(req.CSV), 0644)
	if err != nil {
		slog.Error("Failed to create CSV file", "err", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	slog.Info("Saved client telemetry to disk", "filename", filename, "userID", req.UserID)

	// Trigger client metrics processing (future expansion)
	go func() {
		metricsURL := "http://metrics:8889/api/process/client"
		payload := fmt.Sprintf(`{"runId":"%s"}`, req.UserID)
		resp, err := http.Post(metricsURL, "application/json", strings.NewReader(payload))
		if err != nil {
			slog.Error("Failed to trigger client metrics processing", "err", err, "runId", req.UserID)
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			slog.Error("Client metrics processing returned non-OK status", "status", resp.Status, "runId", req.UserID)
		} else {
			slog.Info("Successfully triggered client metrics processing", "runId", req.UserID)
		}
	}()

	w.WriteHeader(http.StatusOK)
}

func handleListBenchmarks(w http.ResponseWriter, r *http.Request) {
	files, err := os.ReadDir("/benchmarks")
	if err != nil && !os.IsNotExist(err) {
		slog.Error("Failed to read benchmarks directory", "err", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	var dirs []string
	for _, file := range files {
		if file.IsDir() {
			dirs = append(dirs, file.Name())
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(dirs)
}

func handleListBenchmarkFiles(w http.ResponseWriter, r *http.Request) {
	userId := r.PathValue("userId")
	if userId == "" || strings.Contains(userId, "/") || strings.Contains(userId, "\\") {
		http.Error(w, "Invalid userId", http.StatusBadRequest)
		return
	}

	dirpath := filepath.Join("/benchmarks", userId)
	files, err := os.ReadDir(dirpath)
	if err != nil {
		if os.IsNotExist(err) {
			http.Error(w, "Benchmark not found", http.StatusNotFound)
			return
		}
		slog.Error("Failed to read user benchmark directory", "err", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	var filenames []string
	for _, file := range files {
		if !file.IsDir() {
			filenames = append(filenames, file.Name())
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(filenames)
}

func handleDownloadBenchmarkFile(w http.ResponseWriter, r *http.Request) {
	userId := r.PathValue("userId")
	filename := r.PathValue("filename")
	if userId == "" || filename == "" || strings.Contains(userId, "/") || strings.Contains(userId, "\\") || strings.Contains(filename, "/") || strings.Contains(filename, "\\") {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	filepath := filepath.Join("/benchmarks", userId, filename)
	if _, err := os.Stat(filepath); os.IsNotExist(err) {
		http.Error(w, "File not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Access-Control-Expose-Headers", "Content-Disposition")
	http.ServeFile(w, r, filepath)
}

func handleClearTelemetry(w http.ResponseWriter, r *http.Request) {
	if err := serverState.db.ClearEvents(r.Context()); err != nil {
		slog.Error("Failed to clear events", "err", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
	slog.Info("Measurement events table cleared manually via API")
	w.WriteHeader(http.StatusOK)
}
