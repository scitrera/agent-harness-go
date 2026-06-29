package taskstate

import "errors"

var (
	ErrCorruptState      = errors.New("task state corrupt")
	ErrDependencyCycle   = errors.New("task dependency cycle")
	ErrInvalidApproval   = errors.New("invalid plan approval")
	ErrInvalidTransition = errors.New("invalid task status transition")
	ErrInvalidOwner      = errors.New("invalid task owner")
	ErrMissingDependency = errors.New("missing task dependency")
	ErrPlanExists        = errors.New("plan already exists")
	ErrPlanNotFound      = errors.New("plan not found")
	ErrTaskBlocked       = errors.New("task blocked")
	ErrTaskExists        = errors.New("task already exists")
	ErrTaskNotFound      = errors.New("task not found")
	ErrTaskOwnerMismatch = errors.New("task owner mismatch")
	ErrTaskOwned         = errors.New("task already owned")
)
