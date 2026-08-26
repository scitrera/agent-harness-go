package tui

import (
	"strings"

	"github.com/scitrera/agent-harness-go/pkg/commands"
)

const (
	defaultComposerPlaceholder = "Message, !shell, @path, /command, or F1 for help"
)

var helpText = buildHelpText()

func buildHelpText() string {
	definitions := commands.Definitions(commands.SurfaceTUI)
	names := make([]string, 0, len(definitions))
	for _, definition := range definitions {
		names = append(names, "/"+definition.Name)
	}
	return strings.Join([]string{
		"keyboard help",
		"Enter send | Shift+Enter/Ctrl+J newline",
		"Ctrl+Left/Right jump by word",
		"Up/Down move multiline cursor, then recall/edit message history",
		"Queued messages stay local; recall, edit + Enter, or clear + Enter to remove",
		"Mouse wheel scrolls conversation history | Shift+drag selects text",
		"!command runs bash; command + output are added to conversation context",
		"PgUp/PgDn scroll | Ctrl+PgUp/PgDn ends",
		"Tab/Enter complete | ↑/↓ select | / filter",
		"Esc close/cancel active turn | Ctrl+C/Ctrl+D twice to quit",
		strings.Join(names, " "),
		"Type / for commands or @ to reference workspace paths",
	}, "\n")
}

func (m *model) openHelp() {
	m.selector.clear()
	m.showDrawer(drawerHelp, helpText)
	m.status = "help"
}

func (m *model) openCommandBrowser() {
	m.drawer = drawerNone
	m.drawerContent = ""
	m.composer.SetValue("/")
	m.refreshInputSurface()
	m.status = "browse commands"
}
