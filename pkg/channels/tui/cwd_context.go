package tui

import (
	"strconv"
	"strings"
)

const tuiWorkingDirectoryPrefix = "[TUI working directory: "

func (m model) withWorkingDirectoryContext(text string) string {
	workingDirectory := m.currentWorkingDirectory()
	if workingDirectory == "" || workingDirectory == m.workspaceRoot {
		return text
	}
	return tuiWorkingDirectoryPrefix + strconv.Quote(workingDirectory) +
		"; resolve relative paths here and use absolute paths for file/shell tools]\n\n" + text
}

func stripWorkingDirectoryContext(text string) string {
	if !strings.HasPrefix(text, tuiWorkingDirectoryPrefix) {
		return text
	}
	end := strings.Index(text, "]\n\n")
	if end < 0 {
		return text
	}
	return text[end+len("]\n\n"):]
}
