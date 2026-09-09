// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package provider

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
)

// FailureKind classifies a provider failure so the turn loop can decide whether
// to retry, compact-and-retry, or surface the error immediately.
type FailureKind string

const (
	FailureUnknown         FailureKind = "unknown"
	FailureAuth            FailureKind = "auth"             // 401/403 — surface, do not retry
	FailureRateLimit       FailureKind = "rate_limit"       // 429 — retry with backoff
	FailureOverloaded      FailureKind = "overloaded"       // 503/529 — retry with backoff
	FailureServer          FailureKind = "server"           // 5xx — retry with backoff
	FailureTimeout         FailureKind = "timeout"          // deadline/408 — retry with backoff
	FailureNetwork         FailureKind = "network"          // dial/reset — retry with backoff
	FailureContextOverflow FailureKind = "context_overflow" // prompt too long — compact then retry
	FailureBadRequest      FailureKind = "bad_request"      // malformed request — surface, do not retry
)

// ProviderError wraps a classified provider failure. It preserves the
// ErrProviderHTTP sentinel via Unwrap (so existing errors.Is checks keep
// working) while exposing Kind/Status for recovery decisions.
type ProviderError struct {
	Kind    FailureKind
	Status  int // HTTP status, 0 for transport-level failures
	Body    string
	wrapped error
}

func (e *ProviderError) Error() string {
	if e.Status != 0 {
		return fmt.Sprintf("provider %s: status %d: %s", e.Kind, e.Status, e.Body)
	}
	if e.wrapped != nil {
		return fmt.Sprintf("provider %s: %v", e.Kind, e.wrapped)
	}
	return fmt.Sprintf("provider %s", e.Kind)
}

func (e *ProviderError) Unwrap() error { return e.wrapped }

// Retryable reports whether a fresh, identical request might succeed (transient
// failures). Context overflow is NOT retryable as-is — the caller must shrink
// the request first — so it is handled separately.
func (e *ProviderError) Retryable() bool {
	switch e.Kind {
	case FailureRateLimit, FailureOverloaded, FailureServer, FailureTimeout, FailureNetwork:
		return true
	default:
		return false
	}
}

// httpError builds a classified ProviderError for a non-2xx HTTP response. It
// wraps ErrProviderHTTP so callers using errors.Is(err, ErrProviderHTTP) still
// match.
func httpError(status int, body string) *ProviderError {
	return &ProviderError{
		Kind:    classifyHTTP(status, body),
		Status:  status,
		Body:    body,
		wrapped: fmt.Errorf("%w: status %d: %s", ErrProviderHTTP, status, body),
	}
}

// classifyHTTP maps an HTTP status + response body to a FailureKind. The body is
// scanned for context-length signals first, since providers report those on
// both 400 and 429.
func classifyHTTP(status int, body string) FailureKind {
	if looksLikeContextOverflow(body) {
		return FailureContextOverflow
	}
	switch status {
	case 400, 422:
		return FailureBadRequest
	case 401, 403:
		return FailureAuth
	case 408:
		return FailureTimeout
	case 413:
		return FailureContextOverflow
	case 429:
		return FailureRateLimit
	case 503, 529:
		return FailureOverloaded
	}
	if status >= 500 {
		return FailureServer
	}
	return FailureUnknown
}

// classifyTransport classifies a client.Do error (no HTTP response). context
// cancellation is returned unwrapped so the turn loop's cancellation handling is
// unaffected.
func classifyTransport(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return err // caller-driven cancel, not a provider failure
	}
	kind := FailureNetwork
	if errors.Is(err, context.DeadlineExceeded) {
		kind = FailureTimeout
	} else {
		var nerr net.Error
		if errors.As(err, &nerr) && nerr.Timeout() {
			kind = FailureTimeout
		}
	}
	return &ProviderError{Kind: kind, wrapped: err}
}

var contextOverflowSignals = []string{
	"context length",
	"context_length_exceeded",
	"maximum context",
	"context window",
	"too many tokens",
	"reduce the length",
	"prompt is too long",
	"input is too long",
	"maximum_tokens",
	"max_tokens",
	"please reduce",
}

func looksLikeContextOverflow(body string) bool {
	if body == "" {
		return false
	}
	lower := strings.ToLower(body)
	for _, sig := range contextOverflowSignals {
		if strings.Contains(lower, sig) {
			return true
		}
	}
	return false
}
