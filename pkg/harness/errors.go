package harness

import "errors"

var (
	ErrMissingThreadID     = errors.New("harness: thread id required")
	ErrMissingHistoryStore = errors.New("harness: history store required")
)
