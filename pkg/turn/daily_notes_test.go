package turn

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/contextpack"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

func dailyNotesWorkspace(t *testing.T, now time.Time, content string) string {
	t.Helper()
	root := t.TempDir()
	mem := filepath.Join(root, "memory")
	if err := os.MkdirAll(mem, 0o755); err != nil {
		t.Fatal(err)
	}
	name := now.UTC().Format("2006-01-02") + ".md"
	if err := os.WriteFile(filepath.Join(mem, name), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return root
}

func hasMessageContaining(msgs []protocol.ChatMessage, id, substr string) bool {
	for _, m := range msgs {
		if m.ID != id {
			continue
		}
		for _, part := range m.Content {
			if tp, ok := part.AsText(); ok && strings.Contains(tp.Text, substr) {
				return true
			}
		}
	}
	return false
}

func Test_Runner_Run_injects_daily_notes_on_new_thread(t *testing.T) {
	now := time.Date(2026, 6, 22, 9, 0, 0, 0, time.UTC)
	root := dailyNotesWorkspace(t, now, "shipped the streaming feature")
	provider := &fakeProvider{}
	r, err := NewRunner(Config{
		Store:          &fakeStore{},
		Loader:         fakeLoader{},
		Provider:       provider,
		Assembler:      contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 8}),
		DailyNotes:     true,
		DailyNotesDir:  root,
		DailyNotesDays: 2,
		Now:            func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	if _, err := r.Run(context.Background(), protocol.MessageAddress{ThreadID: "t1"}, userMessage(t, "hi")); err != nil {
		t.Fatalf("run: %v", err)
	}
	if !hasMessageContaining(provider.request.Messages, "daily-notes", "shipped the streaming feature") {
		t.Fatalf("daily notes not injected on new thread: %#v", provider.request.Messages)
	}
}

func Test_Runner_Run_skips_daily_notes_on_continued_thread(t *testing.T) {
	now := time.Date(2026, 6, 22, 9, 0, 0, 0, time.UTC)
	root := dailyNotesWorkspace(t, now, "shipped the streaming feature")
	prior, err := protocol.NewTextPart("earlier message")
	if err != nil {
		t.Fatal(err)
	}
	provider := &fakeProvider{}
	r, err := NewRunner(Config{
		Store:          &fakeStore{messages: []protocol.ChatMessage{{ID: "m0", Role: protocol.RoleUser, Content: []protocol.ContentPart{prior}}}},
		Loader:         fakeLoader{},
		Provider:       provider,
		Assembler:      contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 8}),
		DailyNotes:     true,
		DailyNotesDir:  root,
		DailyNotesDays: 2,
		Now:            func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	if _, err := r.Run(context.Background(), protocol.MessageAddress{ThreadID: "t1"}, userMessage(t, "hi")); err != nil {
		t.Fatalf("run: %v", err)
	}
	if hasMessageContaining(provider.request.Messages, "daily-notes", "shipped the streaming feature") {
		t.Fatalf("daily notes should not inject on a continued thread: %#v", provider.request.Messages)
	}
}
