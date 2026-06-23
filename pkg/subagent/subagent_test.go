package subagent

import (
	"context"
	"errors"
	"testing"
)

type fakeRunner struct{ gotDepth int }

func (f *fakeRunner) RunSubagent(_ context.Context, req Request) (Result, error) {
	f.gotDepth = req.Depth
	return Result{Text: "ok"}, nil
}

func TestRefErrNoRunnerUntilSet(t *testing.T) {
	var ref Ref
	if _, err := ref.RunSubagent(context.Background(), Request{Task: "x"}); !errors.Is(err, ErrNoRunner) {
		t.Fatalf("expected ErrNoRunner, got %v", err)
	}
	fr := &fakeRunner{}
	ref.Set(fr)
	res, err := ref.RunSubagent(context.Background(), Request{Task: "x", Depth: 1})
	if err != nil || res.Text != "ok" {
		t.Fatalf("after Set: %v %q", err, res.Text)
	}
	if fr.gotDepth != 1 {
		t.Fatalf("request not forwarded: depth=%d", fr.gotDepth)
	}
}

func TestDepthContext(t *testing.T) {
	ctx := context.Background()
	if Depth(ctx) != 0 {
		t.Fatal("default depth should be 0")
	}
	if Depth(WithDepth(ctx, 3)) != 3 {
		t.Fatal("depth not carried in context")
	}
}
