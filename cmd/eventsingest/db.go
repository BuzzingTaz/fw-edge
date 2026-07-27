package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	pb "github.com/BuzzingTaz/fw-edge-apps/proto"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DBManager encapsulates the database connection pool and related operations.
type DBManager struct {
	pool *pgxpool.Pool
}

// DBConfig holds the configuration for the database connection and retry logic.
type DBConfig struct {
	ConnectionString string
	MaxRetries       int
	RetryInterval    time.Duration
}

// NewDBManager attempts to connect to the database with retry logic.
// It ensures the database is completely reachable before returning the manager.
func NewDBManager(ctx context.Context, cfg DBConfig) (*DBManager, error) {
	// Parse the configuration string into pool configuration
	poolConfig, err := pgxpool.ParseConfig(cfg.ConnectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse database config: %w", err)
	}

	// You can customize pool behavior here (e.g., max connections)
	// poolConfig.MaxConns = 20

	var pool *pgxpool.Pool

	// Retry loop for initial connection
	for attempt := 1; attempt <= cfg.MaxRetries; attempt++ {
		pool, err = pgxpool.NewWithConfig(ctx, poolConfig)
		if err == nil {
			pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			err = pool.Ping(pingCtx)
			cancel()

			if err == nil {
				slog.Info("[DB] Successfully connected to database on attempt", "attempt", attempt)
				return &DBManager{pool: pool}, nil
			}
		}

		slog.Info(fmt.Sprintf("[DB] Connection attempt %d/%d failed: %v. Retrying in %v...",
			attempt, cfg.MaxRetries, err, cfg.RetryInterval))

		select {
		case <-time.After(cfg.RetryInterval):
		case <-ctx.Done():
			return nil, fmt.Errorf("context cancelled during database connection retries: %w", ctx.Err())
		}
	}

	return nil, fmt.Errorf("failed to connect to database after %d attempts: %w", cfg.MaxRetries, err)
}

// Ping allows external services (like a health check endpoint) to verify database status.
func (m *DBManager) Ping(ctx context.Context) error {
	return m.pool.Ping(ctx)
}

// Close gracefully shuts down the connection pool.
func (m *DBManager) Close() {
	if m.pool != nil {
		slog.Info("[DB] Closing database connection pool")
		m.pool.Close()
	}
}

// Pool returns the underlying pgxpool for executing queries or batches.
func (m *DBManager) Pool() *pgxpool.Pool {
	return m.pool
}

// InsertMeasureEvents performs a batch insert of telemetry data into the database.
func (m *DBManager) InsertMeasureEvents(ctx context.Context, events []*pb.MeasureEvent) error {
	if len(events) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	for _, e := range events {
		batch.Queue(
			`INSERT INTO measurement_events
             (meas_time, user_id, task_id, event_type, service_name, payload)
             VALUES ($1, $2, $3, $4, $5, $6)
             ON CONFLICT (task_id, event_type, meas_time) DO NOTHING`,
			e.MeasTime.AsTime(), e.UserId, e.TaskId, e.EventType, e.ServiceName, e.Payload,
		)
	}

	br := m.pool.SendBatch(ctx, batch)
	defer br.Close()

	for i := 0; i < len(events); i++ {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("failed to execute batch insert at index %d: %w", i, err)
		}
	}

	return nil
}

// DumpEventsCSV writes all measurement events for a given userID to a CSV writer.
func (m *DBManager) DumpEventsCSV(ctx context.Context, userID string, w func([]string) error) error {
	query := `
		SELECT meas_time, task_id, event_type, service_name, payload
		FROM measurement_events
		WHERE task_id IN (
			SELECT task_id FROM measurement_events WHERE user_id = $1
		)
		ORDER BY task_id, meas_time
	`
	rows, err := m.pool.Query(ctx, query, userID)
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
		var taskID, eventType, serviceName string
		var payload *string

		if err := rows.Scan(&measTime, &taskID, &eventType, &serviceName, &payload); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		var payloadStr string
		if payload != nil {
			payloadStr = *payload
		}

		record := []string{
			measTime.Format(time.RFC3339Nano),
			taskID,
			eventType,
			serviceName,
			payloadStr,
		}
		if err := w(record); err != nil {
			return err
		}
	}

	return rows.Err()
}

// ClearEvents truncates the measurement_events table.
func (m *DBManager) ClearEvents(ctx context.Context) error {
	_, err := m.pool.Exec(ctx, "TRUNCATE TABLE measurement_events")
	return err
}
