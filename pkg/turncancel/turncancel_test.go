package turncancel

import (
	"context"
	"testing"
)

func TestBeginThenCancelAbortsContext(t *testing.T) {
	c := New()
	ctx, done := c.Begin(context.Background(), "t1")
	defer done()
	if !c.Cancel("t1") {
		t.Fatal("Cancel should report the active turn")
	}
	select {
	case <-ctx.Done():
	default:
		t.Fatal("turn context not cancelled")
	}
}

func TestCancelUnknownReturnsFalse(t *testing.T) {
	if New().Cancel("nope") {
		t.Fatal("Cancel of unknown task should be false")
	}
}

func TestDoneDeregisters(t *testing.T) {
	c := New()
	_, done := c.Begin(context.Background(), "t1")
	done()
	if c.Cancel("t1") {
		t.Fatal("Cancel after done should be false")
	}
}

func TestEmptyTaskIDIsNoop(t *testing.T) {
	c := New()
	parent := context.Background()
	ctx, done := c.Begin(parent, "")
	done()
	if ctx != parent {
		t.Fatal("empty task id should return the parent context unchanged")
	}
}

func TestNilCancellerSafe(t *testing.T) {
	var c *Canceller
	parent := context.Background()
	ctx, done := c.Begin(parent, "t1")
	done()
	if ctx != parent {
		t.Fatal("nil canceller should return parent context")
	}
	if c.Cancel("t1") {
		t.Fatal("nil canceller Cancel should be false")
	}
}
