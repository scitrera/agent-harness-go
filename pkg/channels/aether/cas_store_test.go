package aether

import (
	"context"
	"errors"
	"testing"
	"time"

	sdk "github.com/scitrera/aether/sdk/go/aether"
)

type fakeKVOperations struct {
	getResponse *sdk.KVResponse
	getErr      error
	getOptions  sdk.KVGetOptions

	setNXApplied   bool
	setNXErr       error
	setNXKey       string
	setNXValue     []byte
	setNXScope     sdk.KVScope
	setNXWorkspace string
	setNXTTL       time.Duration
	setNXTimeout   time.Duration

	casApplied   bool
	casErr       error
	casKey       string
	casExpected  []byte
	casValue     []byte
	casScope     sdk.KVScope
	casWorkspace string
	casTTL       time.Duration
	casTimeout   time.Duration
}

func (f *fakeKVOperations) GetSync(_ context.Context, opts sdk.KVGetOptions) (*sdk.KVResponse, error) {
	f.getOptions = opts
	return f.getResponse, f.getErr
}

func (f *fakeKVOperations) SetNXSync(
	_ context.Context,
	key string,
	value []byte,
	scope sdk.KVScope,
	_ string,
	workspace string,
	ttl, timeout time.Duration,
) (bool, error) {
	f.setNXKey = key
	f.setNXValue = append([]byte(nil), value...)
	f.setNXScope = scope
	f.setNXWorkspace = workspace
	f.setNXTTL = ttl
	f.setNXTimeout = timeout
	return f.setNXApplied, f.setNXErr
}

func (f *fakeKVOperations) CompareAndSetSync(
	_ context.Context,
	key string,
	expected, value []byte,
	scope sdk.KVScope,
	_ string,
	workspace string,
	ttl, timeout time.Duration,
) (bool, error) {
	f.casKey = key
	f.casExpected = append([]byte(nil), expected...)
	f.casValue = append([]byte(nil), value...)
	f.casScope = scope
	f.casWorkspace = workspace
	f.casTTL = ttl
	f.casTimeout = timeout
	return f.casApplied, f.casErr
}

func TestKVBlobStoreReadsWorkspaceExclusiveValueAndMiss(t *testing.T) {
	timeout := 3 * time.Second
	kv := &fakeKVOperations{getResponse: &sdk.KVResponse{Success: true, Value: []byte("state")}}
	store, err := NewKVBlobStore(kv, "routing-workspace", timeout)
	if err != nil {
		t.Fatal(err)
	}
	value, found, err := store.Read(context.Background(), "session/key")
	if err != nil {
		t.Fatal(err)
	}
	if !found || string(value) != "state" {
		t.Fatalf("read = %q, %v", value, found)
	}
	value[0] = 'X'
	if string(kv.getResponse.Value) != "state" {
		t.Fatalf("read leaked response bytes: %q", kv.getResponse.Value)
	}
	if kv.getOptions.Scope != sdk.KVScopeWorkspaceExclusive || kv.getOptions.Workspace != "routing-workspace" || kv.getOptions.Timeout != timeout {
		t.Fatalf("get options = %#v", kv.getOptions)
	}

	kv.getResponse = &sdk.KVResponse{Success: true}
	value, found, err = store.Read(context.Background(), "session/missing")
	if err != nil || found || value != nil {
		t.Fatalf("missing read = %q, %v, %v", value, found, err)
	}
}

func TestKVBlobStoreRejectsFailedRead(t *testing.T) {
	store, err := NewKVBlobStore(&fakeKVOperations{getResponse: &sdk.KVResponse{Success: false}}, "workspace", 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Read(context.Background(), "key"); err == nil {
		t.Fatal("failed Aether response unexpectedly treated as a miss")
	}
}

func TestKVBlobStoreCreatesAndSwapsPermanentValues(t *testing.T) {
	timeout := 2 * time.Second
	kv := &fakeKVOperations{setNXApplied: true, casApplied: true}
	store, err := NewKVBlobStore(kv, "routing-workspace", timeout)
	if err != nil {
		t.Fatal(err)
	}
	created, err := store.Create(context.Background(), "session/key", []byte("first"))
	if err != nil || !created {
		t.Fatalf("create = %v, %v", created, err)
	}
	if kv.setNXKey != "session/key" || string(kv.setNXValue) != "first" || kv.setNXScope != sdk.KVScopeWorkspaceExclusive || kv.setNXWorkspace != "routing-workspace" || kv.setNXTTL != 0 || kv.setNXTimeout != timeout {
		t.Fatalf("set-nx call = %#v", kv)
	}

	swapped, err := store.CompareAndSwap(context.Background(), "session/key", []byte("first"), []byte("second"))
	if err != nil || !swapped {
		t.Fatalf("compare-and-swap = %v, %v", swapped, err)
	}
	if kv.casKey != "session/key" || string(kv.casExpected) != "first" || string(kv.casValue) != "second" || kv.casScope != sdk.KVScopeWorkspaceExclusive || kv.casWorkspace != "routing-workspace" || kv.casTTL != 0 || kv.casTimeout != timeout {
		t.Fatalf("CAS call = %#v", kv)
	}
}

func TestKVBlobStorePropagatesAtomicOperationErrors(t *testing.T) {
	want := errors.New("gateway unavailable")
	kv := &fakeKVOperations{setNXErr: want, casErr: want}
	store, err := NewKVBlobStore(kv, "workspace", 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Create(context.Background(), "key", []byte("value")); !errors.Is(err, want) {
		t.Fatalf("create error = %v", err)
	}
	if _, err := store.CompareAndSwap(context.Background(), "key", []byte("old"), []byte("new")); !errors.Is(err, want) {
		t.Fatalf("CAS error = %v", err)
	}
}
