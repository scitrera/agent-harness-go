// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package acp

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

// initWithCaps drives initialize (advertising fs+terminal) -> session/new and
// returns the registered session.
func initWithCaps(t *testing.T, ch *Channel, tc *testClient) *session {
	t.Helper()
	caps := json.RawMessage(`{"fs":{"readTextFile":true,"writeTextFile":true},"terminal":true}`)
	tc.send(t, json.RawMessage("1"), methodInitialize, initializeParams{ProtocolVersion: 1, ClientCapabilities: caps})
	tc.next(t)
	tc.send(t, json.RawMessage("2"), methodSessionNew, newSessionParams{Cwd: "/work"})
	var ns newSessionResult
	if err := json.Unmarshal(tc.next(t).Result, &ns); err != nil {
		t.Fatalf("new session: %v", err)
	}
	return ch.sessionByID(ns.SessionID)
}

// TestTurnContextAttachesDelegates asserts that with fs+terminal caps and a known
// session thread, TurnContext carries both delegates; an unknown thread is
// returned unchanged.
func TestTurnContextAttachesDelegates(t *testing.T) {
	ch, tc, cleanup := newTestPair(t)
	defer cleanup()
	sess := initWithCaps(t, ch, tc)

	ctx := ch.TurnContext(context.Background(), protocol.MessageAddress{ThreadID: sess.threadID})
	if tools.FileDelegateFrom(ctx) == nil {
		t.Fatal("expected FileDelegate on decorated ctx")
	}
	if tools.CommandDelegateFrom(ctx) == nil {
		t.Fatal("expected CommandDelegate on decorated ctx")
	}

	// A sub-agent (or otherwise unknown) thread is not in the session index, so
	// ctx is unchanged and the local workspace is kept.
	base := context.Background()
	got := ch.TurnContext(base, protocol.MessageAddress{ThreadID: "unknown-thread"})
	if tools.FileDelegateFrom(got) != nil || tools.CommandDelegateFrom(got) != nil {
		t.Fatal("unknown thread should not carry delegates")
	}
}

// TestTurnContextNoFsCapNoFileDelegate asserts that without fs caps the file
// delegate is not attached (terminal-only client).
func TestTurnContextNoFsCapNoFileDelegate(t *testing.T) {
	ch, tc, cleanup := newTestPair(t)
	defer cleanup()
	caps := json.RawMessage(`{"terminal":true}`)
	tc.send(t, json.RawMessage("1"), methodInitialize, initializeParams{ProtocolVersion: 1, ClientCapabilities: caps})
	tc.next(t)
	tc.send(t, json.RawMessage("2"), methodSessionNew, newSessionParams{})
	var ns newSessionResult
	if err := json.Unmarshal(tc.next(t).Result, &ns); err != nil {
		t.Fatalf("new session: %v", err)
	}
	sess := ch.sessionByID(ns.SessionID)

	ctx := ch.TurnContext(context.Background(), protocol.MessageAddress{ThreadID: sess.threadID})
	if tools.FileDelegateFrom(ctx) != nil {
		t.Fatal("no fs cap: FileDelegate should be absent")
	}
	if tools.CommandDelegateFrom(ctx) == nil {
		t.Fatal("terminal cap: CommandDelegate should be present")
	}
}

// TestFileDelegateReadIssuesRPC asserts the fileDelegate.ReadFile issues an
// fs/read_text_file request over the wire and returns the client's content. The
// session cwd is prepended to the relative tool path (ACP paths are absolute).
func TestFileDelegateReadIssuesRPC(t *testing.T) {
	ch, tc, cleanup := newTestPair(t)
	defer cleanup()
	sess := initWithCaps(t, ch, tc)

	ctx := ch.TurnContext(context.Background(), protocol.MessageAddress{ThreadID: sess.threadID})
	d := tools.FileDelegateFrom(ctx)
	if d == nil {
		t.Fatal("no file delegate")
	}

	type readOut struct {
		content string
		err     error
	}
	out := make(chan readOut, 1)
	go func() {
		content, err := d.ReadFile(context.Background(), "notes.txt", 0)
		out <- readOut{content, err}
	}()

	// The delegate's outbound fs/read_text_file request arrives on the client.
	req := tc.next(t)
	if req.Method != methodReadTextFile {
		t.Fatalf("expected %q, got %q", methodReadTextFile, req.Method)
	}
	var params readTextFileParams
	if err := json.Unmarshal(req.Params, &params); err != nil {
		t.Fatalf("read params: %v", err)
	}
	if params.SessionID != sess.id || params.Path != "/work/notes.txt" {
		t.Fatalf("unexpected read params: %+v", params)
	}
	tc.reply(t, req.ID, readTextFileResult{Content: "file-body"})

	select {
	case r := <-out:
		if r.err != nil {
			t.Fatalf("ReadFile: %v", r.err)
		}
		if r.content != "file-body" {
			t.Fatalf("content = %q, want file-body", r.content)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ReadFile did not return")
	}
}
