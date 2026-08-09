package aether

import (
	"context"
	"errors"
	"sync"

	sdk "github.com/scitrera/aether/sdk/go/aether"
)

// TaskAssignmentRouter lets independent OSS execution features share the
// Aether SDK's single OnTaskAssignment callback. Handlers must ignore task
// types they do not own.
type TaskAssignmentRouter struct {
	mu       sync.RWMutex
	handlers []sdk.TaskAssignmentHandler
}

func NewTaskAssignmentRouter() *TaskAssignmentRouter { return &TaskAssignmentRouter{} }

func (r *TaskAssignmentRouter) Register(handler sdk.TaskAssignmentHandler) {
	if r == nil || handler == nil {
		return
	}
	r.mu.Lock()
	r.handlers = append(r.handlers, handler)
	r.mu.Unlock()
}

func (r *TaskAssignmentRouter) HandleAssignment(ctx context.Context, assignment *sdk.TaskAssignment) error {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	handlers := append([]sdk.TaskAssignmentHandler(nil), r.handlers...)
	r.mu.RUnlock()
	var joined error
	for _, handler := range handlers {
		joined = errors.Join(joined, handler(ctx, assignment))
	}
	return joined
}
