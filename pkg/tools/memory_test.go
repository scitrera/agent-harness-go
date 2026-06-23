package tools

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
)

type fakeRecaller struct {
	hits []MemoryHit
}

func (f fakeRecaller) Recall(_ context.Context, _ MemoryAuthority, _ string, _ string, _ int) ([]MemoryHit, error) {
	return f.hits, nil
}

func (f fakeRecaller) GetMemory(_ context.Context, id string) (MemoryHit, error) {
	return MemoryHit{ID: id, Content: "full body of " + id, Score: 0.9}, nil
}

func TestRegisterMemoryToolsAndInvoke(t *testing.T) {
	reg := NewRegistry()
	if err := RegisterMemory(reg, fakeRecaller{hits: []MemoryHit{{ID: "m1", Content: "the user prefers terse replies", Score: 0.6}}}); err != nil {
		t.Fatalf("register: %v", err)
	}

	names := map[string]Descriptor{}
	for _, d := range reg.Descriptors() {
		names[d.Name] = d
	}
	for _, n := range []string{"memory_search", "memory_get"} {
		if names[n].Description == "" || len(names[n].Parameters) == 0 {
			t.Fatalf("%s missing descriptor: %+v", n, names[n])
		}
	}

	res, err := reg.Invoke(context.Background(), Request{CallID: "c1", Name: "memory_search", Arguments: json.RawMessage(`{"query":"prefs"}`)})
	if err != nil {
		t.Fatalf("memory_search: %v", err)
	}
	if !bytes.Contains(res.Payload, []byte("terse replies")) {
		t.Fatalf("memory_search payload missing hit: %s", res.Payload)
	}

	res2, err := reg.Invoke(context.Background(), Request{CallID: "c2", Name: "memory_get", Arguments: json.RawMessage(`{"id":"m9"}`)})
	if err != nil {
		t.Fatalf("memory_get: %v", err)
	}
	if !bytes.Contains(res2.Payload, []byte("full body of m9")) {
		t.Fatalf("memory_get payload missing content: %s", res2.Payload)
	}
}

func TestRegisterMemoryNilRecaller(t *testing.T) {
	if err := RegisterMemory(NewRegistry(), nil); err == nil {
		t.Fatal("expected error for nil recaller")
	}
}
