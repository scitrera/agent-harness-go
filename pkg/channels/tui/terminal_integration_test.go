// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package tui

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/x/ansi"

	"github.com/scitrera/agent-harness-go/pkg/store"
	"github.com/scitrera/agent-harness-go/pkg/threadindex"
)

type synchronizedBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *synchronizedBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(data)
}

func (b *synchronizedBuffer) WriteString(data string) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.WriteString(data)
}

func (b *synchronizedBuffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Len()
}

func TestTerminalProgram_helpLayoutAcrossSizes(t *testing.T) {
	for _, size := range []struct {
		name          string
		width, height int
	}{
		{name: "narrow", width: 40, height: 12},
		{name: "standard", width: 60, height: 20},
		{name: "wide", width: 100, height: 30},
	} {
		t.Run(size.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			stateDir := t.TempDir()
			index, err := threadindex.NewIndex(stateDir, nil)
			if err != nil {
				t.Fatalf("new thread index: %v", err)
			}
			initial, err := newModel(ctx, Config{
				Channel:     NewChannel(),
				Store:       store.NewFileStore("", stateDir),
				Index:       index,
				ModelStatus: fakeModelStatus("accounts/fireworks/models/minimax-m3"),
			})
			if err != nil {
				t.Fatalf("new TUI model: %v", err)
			}

			var output synchronizedBuffer
			program := tea.NewProgram(
				initial,
				tea.WithContext(ctx),
				tea.WithInput(bytes.NewBufferString("/help\r\x03\x03")),
				tea.WithOutput(&output),
				tea.WithEnvironment([]string{"TERM=xterm-256color"}),
				tea.WithWindowSize(size.width, size.height),
				tea.WithoutSignals(),
			)
			final, err := program.Run()
			if err != nil {
				t.Fatalf("run terminal program: %v", err)
			}
			if output.Len() == 0 {
				t.Fatal("terminal renderer produced no output")
			}
			m, ok := final.(model)
			if !ok {
				t.Fatalf("final model type = %T", final)
			}
			if m.drawer != drawerHelp {
				t.Fatalf("final drawer = %v, want help", m.drawer)
			}

			rendered := ansi.Strip(m.render())
			if got := lipgloss.Height(rendered); got != size.height {
				t.Fatalf("rendered height = %d, want %d:\n%s", got, size.height, rendered)
			}
			lines := strings.Split(rendered, "\n")
			for i, line := range lines {
				if got := ansi.StringWidth(line); got > size.width {
					t.Fatalf("line %d width = %d, want <= %d: %q", i, got, size.width, line)
				}
			}
			if !strings.Contains(rendered, "keyboard help") {
				t.Fatalf("help header missing:\n%s", rendered)
			}
			if !strings.HasPrefix(lines[len(lines)-1], "status help") {
				t.Fatalf("last row is not help status: %q", lines[len(lines)-1])
			}
		})
	}
}

func TestTerminalProgram_tabCompletesSlashCommand(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	stateDir := t.TempDir()
	index, err := threadindex.NewIndex(stateDir, nil)
	if err != nil {
		t.Fatalf("new thread index: %v", err)
	}
	initial, err := newModel(ctx, Config{
		Channel: NewChannel(),
		Store:   store.NewFileStore("", stateDir),
		Index:   index,
	})
	if err != nil {
		t.Fatalf("new TUI model: %v", err)
	}

	var output synchronizedBuffer
	program := tea.NewProgram(
		initial,
		tea.WithContext(ctx),
		tea.WithInput(bytes.NewBufferString("/att\t\x03\x03")),
		tea.WithOutput(&output),
		tea.WithEnvironment([]string{"TERM=xterm-256color"}),
		tea.WithWindowSize(60, 20),
		tea.WithoutSignals(),
	)
	final, err := program.Run()
	if err != nil {
		t.Fatalf("run terminal program: %v", err)
	}
	m := final.(model)
	if got := m.composer.Value(); got != "/attach " {
		t.Fatalf("composer value = %q, want /attach completion", got)
	}
}
