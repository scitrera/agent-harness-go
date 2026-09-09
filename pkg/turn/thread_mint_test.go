// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package turn

import (
	"context"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/contextpack"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

// oboAuthority is a turn Authority hook that supplies a valid OBO identity.
// Backends that require it receive it; local OSS registrars may not need it.
func oboAuthority(_ protocol.MessageAddress, _ protocol.ChatMessage) tools.MemoryAuthority {
	return tools.MemoryAuthority{GrantID: "grant-1", SubjectType: "user", SubjectID: "u@example.com"}
}

// Test_Runner_Run_mints_thread_via_registrar_when_absent: a turn that arrives
// with NO thread id (a new chat from a "dumb" client) BUT carrying an OBO identity
// has one MINTED by the backend ThreadRegistrar, and the turn adopts it — the
// session, commit, and the returned message all carry the canonical id so the
// client can continue. The OBO identity is required (the mint is a per-user backend
// write); see Test_Runner_Run_mints_locally_when_no_obo for the no-identity path.
func Test_Runner_Run_mints_thread_via_registrar_when_absent(t *testing.T) {
	mem := &fakeMemory{}
	r, err := NewRunner(Config{
		Store:            &fakeStore{},
		Loader:           fakeLoader{},
		Provider:         &fakeProvider{},
		Assembler:        contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 8}),
		Memory:           mem,
		MemoryAutoCommit: true,
		Authority:        oboAuthority,
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	userPart, err := protocol.NewTextPart("hello")
	if err != nil {
		t.Fatalf("user text: %v", err)
	}

	finalized, err := r.Run(context.Background(),
		protocol.MessageAddress{WorkspaceID: "ws1"}, // no ThreadID
		protocol.ChatMessage{ID: "u1", Role: protocol.RoleUser, Content: []protocol.ContentPart{userPart}})
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	// The backend minted a canonical id and the turn adopted it.
	if finalized.Addr.ThreadID != "mem-thread-1" {
		t.Fatalf("finalized thread = %q, want mem-thread-1 (adopted mint)", finalized.Addr.ThreadID)
	}
	// The mint request carried NO id (empty → mint) and origin "chat".
	if len(mem.ensured) != 1 || mem.ensured[0].ThreadID != "" ||
		mem.ensured[0].Origin != "chat" || mem.ensured[0].WorkspaceID != "ws1" {
		t.Fatalf("mint spec = %#v, want one {id:empty, origin:chat, ws:ws1}", mem.ensured)
	}
	// The turn committed onto the minted thread (not an empty id).
	if mem.appendThread != "mem-thread-1" {
		t.Fatalf("commit thread = %q, want mem-thread-1", mem.appendThread)
	}
}

// A registrar receives even an empty authority because OSS/local backends do not
// require OBO. Authorization policy belongs to the backend; if it rejects the
// call, the runner falls back to a local id (covered below).
func Test_Runner_Run_mints_via_local_registrar_without_obo(t *testing.T) {
	mem := &fakeMemory{}
	r, err := NewRunner(Config{
		Store:            &fakeStore{},
		Loader:           fakeLoader{},
		Provider:         &fakeProvider{},
		Assembler:        contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 8}),
		Memory:           mem,
		MemoryAutoCommit: true,
		NewThreadID:      func() string { return "local-99" },
		// No Authority → zero OBO authority for the turn.
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	userPart, err := protocol.NewTextPart("hello")
	if err != nil {
		t.Fatalf("user text: %v", err)
	}

	finalized, err := r.Run(context.Background(),
		protocol.MessageAddress{WorkspaceID: "ws1"}, // no ThreadID, no OBO
		protocol.ChatMessage{ID: "u1", Role: protocol.RoleUser, Content: []protocol.ContentPart{userPart}})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if finalized.Addr.ThreadID != "mem-thread-1" {
		t.Fatalf("finalized thread = %q, want mem-thread-1 (registrar mint without OBO)", finalized.Addr.ThreadID)
	}
	if len(mem.ensured) != 1 {
		t.Fatalf("EnsureThread calls = %#v, want one", mem.ensured)
	}
}

type rejectingThreadRegistrar struct{ fakeMemory }

func (rejectingThreadRegistrar) EnsureThread(context.Context, tools.MemoryAuthority, ThreadSpec) (string, error) {
	return "", context.Canceled
}

func Test_Runner_Run_mints_locally_when_registrar_rejects(t *testing.T) {
	r, err := NewRunner(Config{
		Store:       &fakeStore{},
		Loader:      fakeLoader{},
		Provider:    &fakeProvider{},
		Assembler:   contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 8}),
		Memory:      &rejectingThreadRegistrar{},
		NewThreadID: func() string { return "local-99" },
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	userPart, err := protocol.NewTextPart("hello")
	if err != nil {
		t.Fatal(err)
	}
	finalized, err := r.Run(context.Background(), protocol.MessageAddress{WorkspaceID: "ws1"},
		protocol.ChatMessage{ID: "u1", Role: protocol.RoleUser, Content: []protocol.ContentPart{userPart}})
	if err != nil {
		t.Fatal(err)
	}
	if finalized.Addr.ThreadID != "local-99" {
		t.Fatalf("finalized thread = %q, want local fallback", finalized.Addr.ThreadID)
	}
}

// Test_Runner_Run_mints_thread_locally_without_registrar: with no ThreadRegistrar
// backend, an absent thread id is minted LOCALLY so an offline/dumb client still
// works. NewThreadID supplies a deterministic id here.
func Test_Runner_Run_mints_thread_locally_without_registrar(t *testing.T) {
	r, err := NewRunner(Config{
		Store:       &fakeStore{},
		Loader:      fakeLoader{},
		Provider:    &fakeProvider{},
		Assembler:   contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 8}),
		NewThreadID: func() string { return "local-42" },
		// No Memory → no ThreadRegistrar.
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	userPart, err := protocol.NewTextPart("hello")
	if err != nil {
		t.Fatalf("user text: %v", err)
	}

	finalized, err := r.Run(context.Background(),
		protocol.MessageAddress{}, // no ThreadID, no backend
		protocol.ChatMessage{ID: "u1", Role: protocol.RoleUser, Content: []protocol.ContentPart{userPart}})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if finalized.Addr.ThreadID != "local-42" {
		t.Fatalf("finalized thread = %q, want local-42 (local mint)", finalized.Addr.ThreadID)
	}
}

// Test_Runner_Run_keeps_supplied_thread_id: a supplied thread id is used verbatim
// — no mint, no EnsureThread call (the platform path where app-server supplies it).
func Test_Runner_Run_keeps_supplied_thread_id(t *testing.T) {
	mem := &fakeMemory{}
	r, err := NewRunner(Config{
		Store:            &fakeStore{},
		Loader:           fakeLoader{},
		Provider:         &fakeProvider{},
		Assembler:        contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 8}),
		Memory:           mem,
		MemoryAutoCommit: true,
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	userPart, err := protocol.NewTextPart("hi")
	if err != nil {
		t.Fatalf("user text: %v", err)
	}

	finalized, err := r.Run(context.Background(),
		protocol.MessageAddress{ThreadID: "given-1", WorkspaceID: "ws"},
		protocol.ChatMessage{ID: "u1", Role: protocol.RoleUser, Content: []protocol.ContentPart{userPart}})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if finalized.Addr.ThreadID != "given-1" {
		t.Fatalf("finalized thread = %q, want given-1 (verbatim)", finalized.Addr.ThreadID)
	}
	if len(mem.ensured) != 0 {
		t.Fatalf("a supplied thread id must not mint, ensured=%#v", mem.ensured)
	}
}
