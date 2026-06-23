package runtime

import "errors"

var (
	ErrMissingTaskSource   = errors.New("runtime: task source required")
	ErrMissingTurnExecutor = errors.New("runtime: turn executor required")
	ErrMissingThreadID     = errors.New("runtime: thread id required")
)
