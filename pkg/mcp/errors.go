package mcp

import (
	"errors"
	"fmt"
)

var (
	ErrInvalidServer   = errors.New("mcp: invalid server")
	ErrInvalidResource = errors.New("mcp: invalid resource")
	ErrUnknownServer   = errors.New("mcp: unknown server")
	ErrProcessExit     = errors.New("mcp: process exited")
	ErrRPC             = errors.New("mcp: json-rpc error")
)

type RPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *RPCError) Error() string {
	return fmt.Sprintf("%s: %d %s", ErrRPC, e.Code, e.Message)
}

func (e *RPCError) Is(target error) bool {
	return target == ErrRPC
}
