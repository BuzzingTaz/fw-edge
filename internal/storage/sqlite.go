package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	pb "github.com/BuzzingTaz/fw-edge-apps/proto"
	_ "github.com/mattn/go-sqlite3"
)

type SQLiteStorage struct {
	db *sql.DB
}

func NewSQLiteStorage(dbPath string) (*SQLiteStorage, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite database: %w", err)
	}

	// Create table if not exists
	createTableQuery := `
	CREATE TABLE IF NOT EXISTS measurement_events (
		meas_time DATETIME NOT NULL,
		user_id TEXT NOT NULL,
		task_id TEXT NOT NULL,
		event_type TEXT NOT NULL,
		service_name TEXT NOT NULL,
		payload TEXT,
		PRIMARY KEY (task_id, event_type, meas_time)
	);
	`
	if _, err := db.Exec(createTableQuery); err != nil {
		return nil, fmt.Errorf("failed to create table: %w", err)
	}

	return &SQLiteStorage{db: db}, nil
}

func (s *SQLiteStorage) InsertMeasureEvents(ctx context.Context, events []*pb.MeasureEvent) error {
	if len(events) == 0 {
		return nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	stmt, err := tx.PrepareContext(ctx, `
		INSERT OR IGNORE INTO measurement_events (meas_time, user_id, task_id, event_type, service_name, payload)
		VALUES (?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, e := range events {
		var payloadStr string
		if len(e.Payload) > 0 {
			encoded, err := json.Marshal(e.Payload)
			if err != nil {
				tx.Rollback()
				return fmt.Errorf("failed to encode payload: %w", err)
			}
			payloadStr = string(encoded)
		}

		_, err := stmt.ExecContext(ctx, e.MeasTime.AsTime(), e.UserId, e.TaskId, e.EventType, e.ServiceName, payloadStr)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to insert event: %w", err)
		}
	}

	return tx.Commit()
}

func (s *SQLiteStorage) DumpEventsCSV(ctx context.Context, userID string, w func([]string) error) error {
	query := `
		SELECT meas_time, task_id, event_type, service_name, payload
		FROM measurement_events
		WHERE task_id IN (
			SELECT task_id FROM measurement_events WHERE user_id = ?
		)
		ORDER BY task_id, meas_time
	`
	rows, err := s.db.QueryContext(ctx, query, userID)
	if err != nil {
		return fmt.Errorf("failed to query events: %w", err)
	}
	defer rows.Close()

	// Write CSV Header
	if err := w([]string{"meas_time", "task_id", "event_type", "service_name", "payload"}); err != nil {
		return err
	}

	for rows.Next() {
		var measTime time.Time
		var taskID, eventType, serviceName, payload string

		if err := rows.Scan(&measTime, &taskID, &eventType, &serviceName, &payload); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		record := []string{
			measTime.Format(time.RFC3339Nano),
			taskID,
			eventType,
			serviceName,
			payload,
		}
		if err := w(record); err != nil {
			return err
		}
	}

	return rows.Err()
}

func (s *SQLiteStorage) ClearEvents(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, "DELETE FROM measurement_events")
	return err
}

func (s *SQLiteStorage) Close() error {
	return s.db.Close()
}
