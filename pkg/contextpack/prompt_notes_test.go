// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package contextpack

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/promptnotes"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

type promptNoteProviderStub struct {
	workspace string
	notes     []promptnotes.Note
	err       error
	calls     int
}

func (p *promptNoteProviderStub) LoadWorkspace(_ context.Context, workspaceID string) ([]promptnotes.Note, error) {
	p.workspace = workspaceID
	p.calls++
	return p.notes, p.err
}

func TestAssemblerPreparedPromptNotesAreStableAcrossBuilds(t *testing.T) {
	provider := &promptNoteProviderStub{notes: []promptnotes.Note{
		{Key: "stable", Content: "revision one", Enabled: true, SchemaVersion: 1},
	}}
	assembler := NewAssembler(Config{PromptNotes: provider})
	ctx, err := assembler.Prepare(WithWorkspaceID(context.Background(), "project-a"))
	if err != nil {
		t.Fatal(err)
	}
	provider.notes[0].Content = "revision two"
	first, err := assembler.Build(ctx, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	second, err := assembler.Build(ctx, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if provider.calls != 1 {
		t.Fatalf("provider calls = %d", provider.calls)
	}
	for _, messages := range [][]protocol.ChatMessage{first, second} {
		text := promptText(t, messages)
		if !strings.Contains(text, "revision one") || strings.Contains(text, "revision two") {
			t.Fatalf("prepared snapshot changed:\n%s", text)
		}
	}
}

func TestAssemblerLoadsWorkspacePromptNotesDeterministically(t *testing.T) {
	provider := &promptNoteProviderStub{notes: []promptnotes.Note{
		{Key: "z-last", Title: "Last", Content: "last instruction", Enabled: true, SchemaVersion: 1},
		{Key: "off", Content: "must not appear", Enabled: false, SchemaVersion: 1},
		{Key: "a-first", Content: "first instruction", Enabled: true, SchemaVersion: 1},
	}}
	ctx := WithWorkspaceID(context.Background(), "project-a")
	messages, err := NewAssembler(Config{PromptNotes: provider}).Build(ctx, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	text := promptText(t, messages)
	if provider.workspace != "project-a" {
		t.Fatalf("workspace = %q", provider.workspace)
	}
	first := strings.Index(text, `"key":"a-first"`)
	last := strings.Index(text, `"key":"z-last"`)
	if first < 0 || last < 0 || first >= last || strings.Contains(text, "must not appear") {
		t.Fatalf("prompt-note selection/order wrong:\n%s", text)
	}
}

func TestAssemblerFailsWhenPromptNoteAuthorityFails(t *testing.T) {
	provider := &promptNoteProviderStub{err: errors.New("authoritative store unavailable")}
	_, err := NewAssembler(Config{PromptNotes: provider}).Build(WithWorkspaceID(context.Background(), "project-a"), nil, nil)
	if err == nil || !strings.Contains(err.Error(), `load prompt notes for workspace "project-a"`) || !strings.Contains(err.Error(), "authoritative store unavailable") {
		t.Fatalf("assembly error = %v", err)
	}
}
