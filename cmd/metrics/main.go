package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"time"
)

type Event struct {
	MeasTime    time.Time
	TaskID      string
	EventType   string
	ServiceName string
	Payload     string
}

type Payload struct {
	Timestamp string `json:"timestamp"`
}

//	var orderedEvents = []string{
//		"clientif_new_frame_received",
//		"scheduler3_frame_received",
//		"scheduler3_frame_sent_compute",
//		"compute2_frame_received",
//		"compute2_frame_decoded",
//		"compute2_inference_complete",
//		"compute2_result_yielded",
//		"scheduler3_result_received_compute",
//		"scheduler3_result_sent",
//		"clientif_results_reached",
//		"clientif_results_sent",
//	}
var orderedEvents = []string{
	"clientif_new_frame_received",
	"scheduler4_frame_received",
	"scheduler4_frame_sent_compute",
	"compute3_frame_received",
	"compute3_frame_decoded",
	"compute3_inference_complete",
	"compute3_result_yielded",
	"scheduler4_result_received_compute",
	"scheduler4_result_sent",
	"clientif_results_reached",
	"clientif_results_sent",
}

func main() {
	if len(os.Args) >= 3 {
		// CLI Mode
		inputPath := os.Args[1]
		outputPath := os.Args[2]
		if err := processEdgeCSV(inputPath, outputPath); err != nil {
			log.Fatalf("Failed to process edge CSV: %v", err)
		}
		fmt.Printf("Successfully processed telemetry to %s\n", outputPath)
		return
	}

	// Server Mode
	slog.Info("Starting metrics HTTP server on :8889")
	http.HandleFunc("/api/process/edge", handleProcessEdge)
	http.HandleFunc("/api/process/client", handleProcessClient)
	if err := http.ListenAndServe(":8889", nil); err != nil {
		slog.Error("Metrics HTTP server failed", "err", err)
	}
}

func handleProcessEdge(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		RunID string `json:"runId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.RunID == "" {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}

	inputPath := filepath.Join("/benchmarks", req.RunID, fmt.Sprintf("edge_telemetry_%s.csv", req.RunID))
	outputPath := filepath.Join("/benchmarks", req.RunID, fmt.Sprintf("%s_processed.csv", req.RunID))

	if err := processEdgeCSV(inputPath, outputPath); err != nil {
		slog.Error("Failed to process edge CSV", "runId", req.RunID, "err", err)
		http.Error(w, "Failed to process edge CSV", http.StatusInternalServerError)
		return
	}

	slog.Info("Processed edge telemetry", "runId", req.RunID, "outputPath", outputPath)
	w.WriteHeader(http.StatusOK)
}

func handleProcessClient(w http.ResponseWriter, r *http.Request) {
	// Stub for future client metrics processing
	w.WriteHeader(http.StatusOK)
}

func processEdgeCSV(inputPath, outputPath string) error {
	file, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("failed to open input file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return fmt.Errorf("failed to read CSV: %w", err)
	}

	if len(records) < 2 {
		return fmt.Errorf("no data found")
	}

	eventsByTask := make(map[string][]Event)
	var testStartTime time.Time

	for i, row := range records {
		if i == 0 {
			continue // skip header
		}
		if len(row) < 5 {
			continue
		}

		measTime, err := time.Parse(time.RFC3339Nano, row[0])
		if err != nil {
			log.Printf("Invalid time format on row %d: %v", i+1, err)
			continue
		}

		taskID := row[1]
		eventType := row[2]
		serviceName := row[3]
		payload := row[4]

		if eventType == "clientif_new_frame_received" {
			if testStartTime.IsZero() || measTime.Before(testStartTime) {
				testStartTime = measTime
			}
		}

		eventsByTask[taskID] = append(eventsByTask[taskID], Event{
			MeasTime:    measTime,
			TaskID:      taskID,
			EventType:   eventType,
			ServiceName: serviceName,
			Payload:     payload,
		})
	}

	type TaskSummary struct {
		TaskID    string
		StartTime time.Time
	}
	var summaries []TaskSummary
	for taskID, evs := range eventsByTask {
		sort.Slice(evs, func(i, j int) bool {
			return evs[i].MeasTime.Before(evs[j].MeasTime)
		})
		summaries = append(summaries, TaskSummary{TaskID: taskID, StartTime: evs[0].MeasTime})
	}
	sort.Slice(summaries, func(i, j int) bool {
		return summaries[i].StartTime.Before(summaries[j].StartTime)
	})

	outFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer outFile.Close()

	writer := csv.NewWriter(outFile)
	defer writer.Flush()

	headers := []string{"time_since_start_ms", "timestamp", "task_id", "e2e_latency_ms"}
	for i := 0; i < len(orderedEvents)-1; i++ {
		headers = append(headers, fmt.Sprintf("%s_to_%s_ms", orderedEvents[i], orderedEvents[i+1]))
	}
	if err := writer.Write(headers); err != nil {
		return fmt.Errorf("failed to write headers: %w", err)
	}

	for _, summary := range summaries {
		taskID := summary.TaskID
		evs := eventsByTask[taskID]

		eventMap := make(map[string]Event)
		for _, e := range evs {
			eventMap[e.EventType] = e
		}

		startEv, hasStart := eventMap["clientif_new_frame_received"]
		endEv, hasEnd := eventMap["clientif_results_sent"]

		if !hasStart || !hasEnd {
			continue // Skip if incomplete end-to-end
		}

		timeSinceStart := startEv.MeasTime.Sub(testStartTime).Milliseconds()
		e2eLatency := endEv.MeasTime.Sub(startEv.MeasTime).Seconds() * 1000.0

		frameTimestamp := ""
		if startEv.Payload != "" {
			var p Payload
			if err := json.Unmarshal([]byte(startEv.Payload), &p); err == nil {
				frameTimestamp = p.Timestamp
			}
		}

		record := []string{
			fmt.Sprintf("%d", timeSinceStart),
			frameTimestamp,
			taskID,
			fmt.Sprintf("%.2f", e2eLatency),
		}

		for i := 0; i < len(orderedEvents)-1; i++ {
			ev1, ok1 := eventMap[orderedEvents[i]]
			ev2, ok2 := eventMap[orderedEvents[i+1]]
			if ok1 && ok2 {
				latency := ev2.MeasTime.Sub(ev1.MeasTime).Seconds() * 1000.0
				record = append(record, fmt.Sprintf("%.2f", latency))
			} else {
				record = append(record, "")
			}
		}

		if err := writer.Write(record); err != nil {
			return fmt.Errorf("failed to write record: %w", err)
		}
	}

	return nil
}
