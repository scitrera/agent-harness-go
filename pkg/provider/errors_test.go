// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package provider

import (
	"context"
	"errors"
	"testing"
)

func TestClassifyHTTP(t *testing.T) {
	cases := []struct {
		status int
		body   string
		want   FailureKind
	}{
		{401, "unauthorized", FailureAuth},
		{403, "forbidden", FailureAuth},
		{429, "rate limit exceeded", FailureRateLimit},
		{400, "invalid 'messages': must alternate", FailureBadRequest},
		{400, "This model's maximum context length is 8192 tokens", FailureContextOverflow},
		{429, "Please reduce the length of the messages", FailureContextOverflow},
		{413, "payload too large", FailureContextOverflow},
		{503, "overloaded", FailureOverloaded},
		{500, "internal error", FailureServer},
		{408, "request timeout", FailureTimeout},
		{418, "teapot", FailureUnknown},
	}
	for _, c := range cases {
		if got := classifyHTTP(c.status, c.body); got != c.want {
			t.Errorf("classifyHTTP(%d, %q) = %s, want %s", c.status, c.body, got, c.want)
		}
	}
}

func TestHTTPErrorPreservesSentinelAndExposesKind(t *testing.T) {
	err := httpError(429, "slow down")
	if !errors.Is(err, ErrProviderHTTP) {
		t.Fatal("classified error must still match ErrProviderHTTP sentinel")
	}
	var pe *ProviderError
	if !errors.As(err, &pe) {
		t.Fatal("expected *ProviderError via errors.As")
	}
	if pe.Kind != FailureRateLimit || pe.Status != 429 {
		t.Fatalf("unexpected classification: %+v", pe)
	}
	if !pe.Retryable() {
		t.Fatal("rate_limit should be retryable")
	}
}

func TestClassifyTransport(t *testing.T) {
	// Canceled context is returned unwrapped (caller-driven, not a provider failure).
	if got := classifyTransport(context.Canceled); !errors.Is(got, context.Canceled) {
		t.Fatalf("canceled should pass through, got %v", got)
	}
	if _, ok := classifyTransport(context.Canceled).(*ProviderError); ok {
		t.Fatal("canceled must not be wrapped as ProviderError")
	}
	// Deadline exceeded classifies as timeout (retryable).
	var pe *ProviderError
	if !errors.As(classifyTransport(context.DeadlineExceeded), &pe) || pe.Kind != FailureTimeout {
		t.Fatalf("deadline should classify as timeout, got %+v", pe)
	}
	if !pe.Retryable() {
		t.Fatal("timeout should be retryable")
	}
	// Context overflow is not retryable (must shrink first).
	if (&ProviderError{Kind: FailureContextOverflow}).Retryable() {
		t.Fatal("context_overflow must not be retryable as-is")
	}
}
