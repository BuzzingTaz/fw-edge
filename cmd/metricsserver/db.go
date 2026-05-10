package main

import (
	"context"
	"fmt"

	pb "github.com/BuzzingTaz/fw-edge-apps/proto"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const connStr = "postgres://postgres:password@127.0.0.1:5432/postgres"

func ConnectDB() (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(context.Background(), connStr)
	if err != nil {
		return nil, err
	}

	if err := pool.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}
	return pool, nil
}

// Batch insert function
func InsertMeasurements(ctx context.Context, pool *pgxpool.Pool, events []*pb.MeasureEvent) error {
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

	br := pool.SendBatch(ctx, batch)
	defer br.Close()
	for i := 0; i < len(events); i++ {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("batch exec %d: %w", i, err)
		}
	}

	return nil
}

