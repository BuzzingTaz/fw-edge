package metrics

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"os"
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

var orderedEvents = []string{
	"clientif_new_frame_received",
	"scheduler3_frame_received",
	"scheduler3_frame_sent_compute",
	"compute2_frame_received",
	"compute2_frame_decoded",
	"compute2_inference_complete",
	"compute2_result_yielded",
	"scheduler3_result_received_compute",
	"scheduler3_result_sent",
	"clientif_results_reached",
	"clientif_results_sent",
}

// ProcessEdgeCSV pivots a raw edge telemetry dump into one row per completed frame.
func ProcessEdgeCSV(inputPath, outputPath string) error {
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
			continue
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
			continue
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

// ProcessClientEdgeCSV joins browser telemetry with a processed edge CSV.
// Rows match on the RTP timestamp: client frameId and edge timestamp.
// Durations stay inside the clock that measured them.
func ProcessClientEdgeCSV(clientPath, edgeProcessedPath, outputPath string) error {
	clientHeader, clientRows, err := readCSV(clientPath)
	if err != nil {
		return fmt.Errorf("failed to read client telemetry: %w", err)
	}
	edgeHeader, edgeRows, err := readCSV(edgeProcessedPath)
	if err != nil {
		return fmt.Errorf("failed to read processed edge telemetry: %w", err)
	}
	if !hasColumn(clientHeader, "frameId") || !hasColumn(edgeHeader, "timestamp") || !hasColumn(edgeHeader, "e2e_latency_ms") {
		return fmt.Errorf("missing join columns in client or edge CSV")
	}

	byFrame := make(map[string]map[string]string, len(clientRows))
	for _, row := range clientRows {
		id := row["frameId"]
		if id != "" {
			byFrame[id] = row
		}
	}

	stageCols := make([]string, 0)
	for _, col := range edgeHeader {
		switch col {
		case "time_since_start_ms", "timestamp", "task_id", "e2e_latency_ms":
			continue
		default:
			stageCols = append(stageCols, col)
		}
	}

	headers := []string{
		"time_since_start_ms",
		"timestamp",
		"task_id",
		"capture_to_offload_ms",
		"offload_to_receive_ms",
		"receive_to_display_ms",
		"client_e2e_receive_ms",
		"client_e2e_display_ms",
		"edge_e2e_ms",
		"outside_edge_ms",
	}
	headers = append(headers, stageCols...)

	outFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer outFile.Close()

	writer := csv.NewWriter(outFile)
	defer writer.Flush()
	if err := writer.Write(headers); err != nil {
		return fmt.Errorf("failed to write headers: %w", err)
	}

	joined := 0
	for _, edge := range edgeRows {
		client, ok := byFrame[edge["timestamp"]]
		if !ok {
			continue
		}

		captureToOffload := deltaMS(client["capture"], client["offload"])
		offloadToReceive := deltaMS(client["offload"], client["receive"])
		receiveToDisplay := deltaMS(client["receive"], client["display"])
		clientE2EReceive := deltaMS(client["capture"], client["receive"])
		clientE2EDisplay := deltaMS(client["capture"], client["display"])
		outsideEdge := ""
		if offloadToReceive != "" && edge["e2e_latency_ms"] != "" {
			outsideEdge = deltaMSNumbers(edge["e2e_latency_ms"], offloadToReceive)
		}

		record := []string{
			edge["time_since_start_ms"],
			edge["timestamp"],
			edge["task_id"],
			captureToOffload,
			offloadToReceive,
			receiveToDisplay,
			clientE2EReceive,
			clientE2EDisplay,
			edge["e2e_latency_ms"],
			outsideEdge,
		}
		for _, col := range stageCols {
			record = append(record, edge[col])
		}
		if err := writer.Write(record); err != nil {
			return fmt.Errorf("failed to write record: %w", err)
		}
		joined++
	}

	if joined == 0 {
		return fmt.Errorf("no frames matched on RTP timestamp")
	}
	return nil
}

func readCSV(path string) ([]string, []map[string]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, nil, err
	}
	if len(records) == 0 {
		return nil, nil, fmt.Errorf("empty file")
	}

	header := records[0]
	rows := make([]map[string]string, 0, len(records)-1)
	for _, record := range records[1:] {
		row := make(map[string]string, len(header))
		for i, col := range header {
			if i < len(record) {
				row[col] = record[i]
			}
		}
		rows = append(rows, row)
	}
	return header, rows, nil
}

func hasColumn(header []string, name string) bool {
	for _, col := range header {
		if col == name {
			return true
		}
	}
	return false
}

func deltaMS(start, end string) string {
	if start == "" || end == "" {
		return ""
	}
	return deltaMSNumbers(start, end)
}

func deltaMSNumbers(start, end string) string {
	var a, b float64
	if _, err := fmt.Sscan(start, &a); err != nil {
		return ""
	}
	if _, err := fmt.Sscan(end, &b); err != nil {
		return ""
	}
	return fmt.Sprintf("%.2f", b-a)
}
