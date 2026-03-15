package metrics

import "context"

type contextKey string

const (
	ContextKeyUserID contextKey = "metrics_user_id"
	ContextKeyTaskID contextKey = "metrics_task_id"
)

// WithUserID returns a context with the user ID attached.
func WithUserID(ctx context.Context, userID string) context.Context {
	return context.WithValue(ctx, ContextKeyUserID, userID)
}

func WithTaskID(ctx context.Context, taskID string) context.Context {
	return context.WithValue(ctx, ContextKeyTaskID, taskID)
}
