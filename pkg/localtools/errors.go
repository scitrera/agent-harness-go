package localtools

import "errors"

var (
	ErrInvalidRoot     = errors.New("localtools: invalid workspace root")
	ErrPathOutsideRoot = errors.New("localtools: path outside workspace root")
	ErrOldTextNotFound = errors.New("localtools: old text not found")
	ErrCommandFailed   = errors.New("localtools: command failed")
	ErrExternalAPIKey  = errors.New("localtools: real external api key is not allowed")
	ErrInvalidFile     = errors.New("localtools: invalid file")
)
