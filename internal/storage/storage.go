package storage

import (
	"context"

	pb "github.com/BuzzingTaz/fw-edge-apps/proto"
)

// TelemetryStorage defines the interface for storing benchmarking telemetry locally on the edge.
type TelemetryStorage interface {
	InsertMeasureEvents(ctx context.Context, events []*pb.MeasureEvent) error
	DumpEventsCSV(ctx context.Context, userID string, w func([]string) error) error
	ClearEvents(ctx context.Context) error
	Close() error
}
