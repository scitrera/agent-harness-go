package tui

const (
	defaultComposerPlaceholder = "Message, @path, /command, or F1 for help"

	helpText = `keyboard help
Enter send | Shift+Enter/Ctrl+J newline
PgUp/PgDn scroll | Ctrl+PgUp/PgDn ends
Tab/Enter complete | ↑/↓ select | / filter
Esc close/cancel | Ctrl+C/Ctrl+D twice to quit
/cd /pwd /threads /tools /approvals /attach
Type / for commands or @ to reference workspace paths`
)

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
