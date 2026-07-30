package tui

import (
	"strings"
	"testing"

	"charm.land/bubbles/v2/viewport"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/x/ansi"

	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/commands"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/threadindex"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

type fakeCommandProvider struct {
	commands []commands.Command
}

func (f fakeCommandProvider) AvailableCommands() []commands.Command {
	return append([]commands.Command(nil), f.commands...)
}

type fakeModelStatus string

func (f fakeModelStatus) ActiveModelName(string) string {
	return string(f)
}

func TestRender_shortDrawerKeepsStatusOnLastTerminalRow(t *testing.T) {
	m := model{
		channel:       NewChannel(),
		viewport:      viewport.New(),
		composer:      newComposer(),
		tailing:       true,
		drawer:        drawerTools,
		drawerContent: "tool activity\ncall-1 tool shell: finished",
	}
	m.resize(60, 20)

	rendered := m.render()
	if got := lipgloss.Height(rendered); got != 20 {
		t.Fatalf("rendered height = %d, want terminal height 20:\n%s", got, ansi.Strip(rendered))
	}
	lines := strings.Split(ansi.Strip(rendered), "\n")
	if got := lines[len(lines)-1]; !strings.HasPrefix(got, "status ") {
		t.Fatalf("last terminal row = %q, want status row", got)
	}
}

func TestFitStatusSegments_keepsLongPrimaryStatus(t *testing.T) {
	got := fitStatusSegments([]string{
		"status attach failed: IDENTITY.md is text/plain; only images are supported",
		"model minimax-m3",
	}, 32)

	if !strings.HasPrefix(got, "status attach failed:") {
		t.Fatalf("primary status was dropped: %q", got)
	}
	if lipgloss.Width(got) > 32 {
		t.Fatalf("status width = %d, want <= 32: %q", lipgloss.Width(got), got)
	}
}

func TestRenderRow_wrapsLongPlainTextWithinTerminalWidth(t *testing.T) {
	m := model{width: 30}
	row := chatRow{
		Kind: rowUser,
		Text: "alpha bravo charlie delta echo foxtrot golf hotel",
	}

	rendered := m.renderRow(row)
	lines := strings.Split(rendered, "\n")
	if len(lines) < 2 {
		t.Fatalf("long row did not wrap: %q", ansi.Strip(rendered))
	}
	for i, line := range lines {
		if got := ansi.StringWidth(line); got > m.width {
			t.Fatalf("line %d width = %d, want <= %d: %q", i, got, m.width, ansi.Strip(line))
		}
	}
	if !strings.Contains(ansi.Strip(rendered), "hotel") {
		t.Fatalf("wrapped row lost trailing text: %q", ansi.Strip(rendered))
	}
}

func TestComposerGrowthKeepsBeginningOfWrappedInputVisible(t *testing.T) {
	m := model{channel: NewChannel(), viewport: viewport.New(), composer: newComposer(), tailing: true}
	m.resize(60, 20)
	input := "This is a deliberately long composer line that should wrap across multiple terminal rows so the beginning remains visible."

	for _, char := range input {
		next, _ := m.updateKey(tea.KeyPressMsg(tea.Key{Code: char, Text: string(char)}))
		m = next.(model)
	}

	rendered := ansi.Strip(m.composer.View())
	if !strings.Contains(rendered, "This is a deliberately") {
		t.Fatalf("composer hid the beginning after growing:\n%s", rendered)
	}
}

func TestUpdateKey_homeMovesComposerCursorInsteadOfScrollback(t *testing.T) {
	m := model{channel: NewChannel(), viewport: viewport.New(), composer: newComposer(), tailing: true}
	m.resize(60, 20)
	m.composer.SetValue("abc")

	next, _ := m.updateKey(tea.KeyPressMsg(tea.Key{Code: tea.KeyHome}))
	updated := next.(model)
	next, _ = updated.updateKey(tea.KeyPressMsg(tea.Key{Code: 'x', Text: "X"}))
	updated = next.(model)

	if got := updated.composer.Value(); got != "Xabc" {
		t.Fatalf("composer value = %q, want Home to produce Xabc", got)
	}
}

func TestUpdateKey_shiftEnterInsertsComposerNewline(t *testing.T) {
	m := model{channel: NewChannel(), viewport: viewport.New(), composer: newComposer(), tailing: true}
	m.resize(60, 20)
	m.composer.SetValue("first")

	next, cmd := m.updateKey(tea.KeyPressMsg(tea.Key{Code: tea.KeyEnter, Mod: tea.ModShift}))
	if cmd == nil {
		// A textarea cursor command is allowed to be nil depending on cursor mode;
		// the value assertion below is the behavior contract.
	}
	updated := next.(model)

	if got := updated.composer.Value(); got != "first\n" {
		t.Fatalf("composer value = %q, want Shift+Enter newline", got)
	}
}

func TestSlashSuggestionsExposeFullCommandCatalog(t *testing.T) {
	m := model{channel: NewChannel(), viewport: viewport.New(), composer: newComposer(), tailing: true}
	m.resize(80, 20)
	m.composer.SetValue("/")
	m.refreshInputSurface()

	if got := len(m.selector.items); got <= maxSelectionRows {
		t.Fatalf("suggestions were truncated before navigation: got %d, want > %d", got, maxSelectionRows)
	}
	for _, want := range []string{"/help", "/commands", "/model", "/thread"} {
		found := false
		for _, item := range m.selector.items {
			if item.Value == want {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("missing command suggestion %q: %+v", want, m.selector.items)
		}
	}
}

func TestSlashSuggestionsIncludeWorkspaceCommandsAndArgumentHint(t *testing.T) {
	m := model{
		channel:  NewChannel(),
		viewport: viewport.New(),
		composer: newComposer(),
		tailing:  true,
		commandSource: fakeCommandProvider{commands: []commands.Command{{
			Name:         "review",
			Description:  "Review the current change",
			ArgumentHint: "<scope>",
		}}},
	}
	m.resize(80, 20)
	m.composer.SetValue("/rev")
	m.refreshInputSurface()

	if got := len(m.selector.items); got != 1 {
		t.Fatalf("workspace suggestion count = %d, want 1: %+v", got, m.selector.items)
	}
	item := m.selector.items[0]
	if item.Value != "/review" || item.Label != "/review <scope>" {
		t.Fatalf("workspace suggestion = %+v", item)
	}
}

func TestSlashSuggestionsCompleteThreadIDs(t *testing.T) {
	m := model{
		channel:  NewChannel(),
		viewport: viewport.New(),
		composer: newComposer(),
		tailing:  true,
		threadID: "thread-alpha",
		threads: []threadindex.Session{
			{ID: "thread-alpha", Title: "Alpha"},
			{ID: "thread-beta", Title: "Beta"},
		},
	}
	m.resize(80, 20)
	m.composer.SetValue("/thread switch thread-b")
	m.refreshInputSurface()

	if got := len(m.selector.items); got != 1 {
		t.Fatalf("thread suggestion count = %d, want 1: %+v", got, m.selector.items)
	}
	if got := m.selector.items[0].Value; got != "/thread switch thread-beta" {
		t.Fatalf("thread completion value = %q", got)
	}
}

func TestUpdateKey_enterAcceptsIncompleteSlashSuggestion(t *testing.T) {
	m := model{channel: NewChannel(), viewport: viewport.New(), composer: newComposer(), tailing: true}
	m.resize(80, 20)
	m.composer.SetValue("/th")
	m.refreshInputSurface()

	next, cmd := m.updateKey(tea.KeyPressMsg(tea.Key{Code: tea.KeyEnter}))
	if cmd != nil {
		t.Fatal("accepting a slash suggestion should not enqueue a turn")
	}
	updated := next.(model)
	if got := updated.composer.Value(); got != "/thread " {
		t.Fatalf("composer value = %q, want completed /thread command", got)
	}
}

func TestUpdateKey_enterExecutesCompletedCommandBeforeSuggestedSubcommand(t *testing.T) {
	m := model{
		channel:          NewChannel(),
		viewport:         viewport.New(),
		composer:         newComposer(),
		tailing:          true,
		pendingApprovals: map[string]approvalRequest{},
		tools:            map[string]toolEntry{},
	}
	m.resize(80, 20)
	m.composer.SetValue("/tools ")
	m.refreshInputSurface()
	if !m.selector.active() {
		t.Fatal("test setup expected /tools subcommand suggestions")
	}

	next, cmd := m.updateKey(tea.KeyPressMsg(tea.Key{Code: tea.KeyEnter}))
	if cmd != nil {
		t.Fatal("local /tools command should execute synchronously")
	}
	updated := next.(model)
	if updated.selector.active() {
		t.Fatal("completed command should execute instead of accepting the first subcommand")
	}
	if updated.drawer != drawerTools {
		t.Fatalf("/tools drawer = %v, want tools", updated.drawer)
	}
}

func TestHelpCommandOpensLocalKeyboardReference(t *testing.T) {
	m := model{
		channel:  NewChannel(),
		viewport: viewport.New(),
		composer: newComposer(),
		tailing:  true,
	}
	m.resize(60, 20)

	next, cmd := m.handleSlash("/help")
	if cmd != nil {
		t.Fatal("local help should not enqueue a turn")
	}
	updated := next.(model)
	if updated.drawer != drawerHelp {
		t.Fatalf("help drawer = %v, want help", updated.drawer)
	}
	for _, want := range []string{"Shift+Enter", "PgUp/PgDn", "Type /"} {
		if !strings.Contains(updated.drawerContent, want) {
			t.Fatalf("help omitted %q:\n%s", want, updated.drawerContent)
		}
	}
}

func TestCommandsCommandOpensSearchableCatalog(t *testing.T) {
	m := model{
		channel:  NewChannel(),
		viewport: viewport.New(),
		composer: newComposer(),
		tailing:  true,
	}
	m.resize(60, 20)

	next, cmd := m.handleSlash("/commands")
	if cmd != nil {
		t.Fatal("command browser should not enqueue a turn")
	}
	updated := next.(model)
	if got := updated.composer.Value(); got != "/" {
		t.Fatalf("composer value = %q, want command browser prefix", got)
	}
	if updated.selector.kind != selectionSlash || len(updated.selector.items) <= maxSelectionRows {
		t.Fatalf("command browser selector = %+v", updated.selector)
	}
}

func TestToolSelectorFiltersWithoutChangingComposer(t *testing.T) {
	m := model{
		channel: NewChannel(),
		tools: map[string]toolEntry{
			"call-shell": {
				Event: tools.ToolEvent{CallID: "call-shell", ToolName: "shell", Status: tools.ToolEventFinished},
				Seen:  2,
			},
			"call-read": {
				Event: tools.ToolEvent{CallID: "call-read", ToolName: "read_file", Status: tools.ToolEventFinished},
				Seen:  1,
			},
		},
		viewport: viewport.New(),
		composer: newComposer(),
		tailing:  true,
	}
	m.resize(60, 20)
	m.openToolSelector(false)

	for _, msg := range []tea.KeyPressMsg{
		tea.KeyPressMsg(tea.Key{Code: '/', Text: "/"}),
		tea.KeyPressMsg(tea.Key{Code: 'r', Text: "r"}),
		tea.KeyPressMsg(tea.Key{Code: 'e', Text: "e"}),
		tea.KeyPressMsg(tea.Key{Code: 'a', Text: "a"}),
		tea.KeyPressMsg(tea.Key{Code: 'd', Text: "d"}),
	} {
		next, _ := m.updateKey(msg)
		m = next.(model)
	}

	if got := m.composer.Value(); got != "" {
		t.Fatalf("selector filter leaked into composer: %q", got)
	}
	if m.selector.query != "read" || len(m.selector.items) != 1 {
		t.Fatalf("filtered selector = query %q items %+v", m.selector.query, m.selector.items)
	}
	if got := m.selector.items[0].Value; got != "call-read" {
		t.Fatalf("filtered item = %q, want call-read", got)
	}
	if rendered := ansi.Strip(m.renderSelection()); !strings.Contains(rendered, "filter: read") {
		t.Fatalf("filter query not rendered:\n%s", rendered)
	}
}

func TestFilteredSelectorFitsSmallTerminalWithNoMatches(t *testing.T) {
	m := model{
		channel: NewChannel(),
		tools: map[string]toolEntry{
			"call-shell": {
				Event: tools.ToolEvent{CallID: "call-shell", ToolName: "shell", Status: tools.ToolEventFinished},
			},
		},
		viewport: viewport.New(),
		composer: newComposer(),
		tailing:  true,
	}
	m.resize(40, 10)
	m.openToolSelector(false)
	m.selector.beginFilter()
	m.selector.appendFilter("missing")
	m.reflowSurfaces()

	rendered := ansi.Strip(m.render())
	if got := lipgloss.Height(rendered); got != 10 {
		t.Fatalf("filtered small-terminal height = %d, want 10:\n%s", got, rendered)
	}
	if !strings.Contains(rendered, "filter: missing") || !strings.Contains(rendered, "no matches") {
		t.Fatalf("empty filtered state missing:\n%s", rendered)
	}
}

func TestApplyEvent_messageFinalReturnsStatusToReady(t *testing.T) {
	part, err := protocol.NewTextPart("done")
	if err != nil {
		t.Fatalf("text part: %v", err)
	}
	m := model{
		channel:          NewChannel(),
		threadID:         "thread-1",
		status:           "queued",
		pendingApprovals: map[string]approvalRequest{},
		tools:            map[string]toolEntry{},
		viewport:         viewport.New(),
		composer:         newComposer(),
		tailing:          true,
	}

	m.applyEvent(channel.Event{
		Type: channel.EventMessageFinal,
		Addr: protocol.MessageAddress{ThreadID: "thread-1", TaskID: "task-1"},
		Message: &protocol.ChatMessage{
			ID:      "assistant-1",
			Role:    protocol.RoleAssistant,
			Addr:    protocol.MessageAddress{ThreadID: "thread-1", TaskID: "task-1"},
			Content: []protocol.ContentPart{part},
		},
	})

	if got := m.status; got != "ready" {
		t.Fatalf("status after final message = %q, want ready", got)
	}
}

func TestStatusBarPrioritizesUsefulCompactSegments(t *testing.T) {
	m := model{
		channel:          NewChannel(),
		modelStatus:      fakeModelStatus("accounts/fireworks/models/minimax-m3"),
		threadID:         "thread-1",
		threads:          []threadindex.Session{{ID: "thread-1", Title: "Design review"}},
		status:           "ready",
		pendingApprovals: map[string]approvalRequest{},
		tools:            map[string]toolEntry{},
		viewport:         viewport.New(),
		width:            60,
	}
	m.viewport.SetWidth(60)
	m.viewport.SetHeight(10)

	status := ansi.Strip(m.renderStatus())
	if !strings.Contains(status, "model minimax-m3") {
		t.Fatalf("status omitted compact model name: %q", status)
	}
	if strings.Contains(status, "ctx unknown") || strings.Contains(status, "accounts/fireworks") {
		t.Fatalf("status retained low-value or unbounded segments: %q", status)
	}
	if got := ansi.StringWidth(status); got != 60 {
		t.Fatalf("status width = %d, want 60", got)
	}
}

func TestApprovalSelectorResolvesSelectedRequestWithKeyboardScope(t *testing.T) {
	resolver := &fakeApprovalResolver{ok: true}
	m := model{
		channel:   NewChannel(),
		approvals: resolver,
		pendingApprovals: map[string]approvalRequest{
			"call-1": {RequestID: "call-1", TaskID: "task-1", Tool: "shell", Status: "pending"},
		},
		tools:    map[string]toolEntry{},
		viewport: viewport.New(),
		composer: newComposer(),
		tailing:  true,
	}
	m.resize(80, 20)
	m.openApprovalSelector()

	next, _ := m.updateKey(tea.KeyPressMsg(tea.Key{Code: 's', Text: "s"}))
	updated := next.(model)

	if resolver.requestID != "call-1" || !resolver.decision.Granted || resolver.decision.Scope != "session" {
		t.Fatalf("approval decision mismatch: %+v", resolver)
	}
	if updated.selector.active() || updated.drawer != drawerNone {
		t.Fatalf("resolved approval UI stayed open: selector=%+v drawer=%v", updated.selector, updated.drawer)
	}
}

func TestToolSelectorOpensSafeDetailView(t *testing.T) {
	event := tools.ToolEvent{
		Status:         tools.ToolEventFinished,
		CallID:         "call-123456789",
		ToolName:       "shell",
		ArgumentsHash:  strings.Repeat("a", 64),
		ArgumentsBytes: 42,
		DurationMS:     8,
		Result:         tools.ResultMetadata{OutputBytes: 12},
	}
	m := model{
		channel:          NewChannel(),
		pendingApprovals: map[string]approvalRequest{},
		tools:            map[string]toolEntry{event.CallID: {Event: event, Seen: 1}},
		viewport:         viewport.New(),
		composer:         newComposer(),
		tailing:          true,
	}
	m.resize(80, 20)
	m.openToolSelector(false)

	next, _ := m.updateKey(tea.KeyPressMsg(tea.Key{Code: tea.KeyEnter}))
	updated := next.(model)

	if updated.selector.active() {
		t.Fatal("tool selector should close after opening details")
	}
	for _, want := range []string{"tool shell", "status: finished", "42 bytes", "sha256=aaaaaaaaaaaa"} {
		if !strings.Contains(updated.drawerContent, want) {
			t.Fatalf("tool detail missing %q:\n%s", want, updated.drawerContent)
		}
	}
}
