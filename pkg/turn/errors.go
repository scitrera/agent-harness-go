package turn

import "errors"

var (
	ErrMissingStore    = errors.New("turn: history store required")
	ErrMissingLoader   = errors.New("turn: bootstrap loader required")
	ErrMissingProvider = errors.New("turn: provider required")
	ErrToolLoopLimit   = errors.New("turn: tool loop limit reached")
)
