package requirements

import (
	"errors"
	"fmt"
)

var (
	ErrInvalidLayer = errors.New("requirements: invalid layer")
	ErrConflict     = errors.New("requirements: conflicting layers")
)

type ConflictError struct {
	Field          string
	ExistingSource Source
	IncomingSource Source
	Message        string
}

func (e *ConflictError) Error() string {
	return fmt.Sprintf(
		"%s: field %q between %s and %s: %s",
		ErrConflict,
		e.Field,
		e.ExistingSource.display(),
		e.IncomingSource.display(),
		e.Message,
	)
}

func (e *ConflictError) Is(target error) bool {
	return target == ErrConflict
}

func conflict(field string, existing Source, incoming Source, message string) error {
	return &ConflictError{
		Field:          field,
		ExistingSource: existing,
		IncomingSource: incoming,
		Message:        message,
	}
}
