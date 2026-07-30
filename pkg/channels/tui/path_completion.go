package tui

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"
)

const maxPathSuggestions = 200

type pathCompletionContext struct {
	input           string
	pathStart       int
	rawPath         string
	marker          string
	quoted          bool
	directoriesOnly bool
}

func (m *model) refreshPathSuggestions() bool {
	completion, ok := pathCompletionForInput(m.composer.Value())
	if !ok {
		if m.selector.kind == selectionPath {
			m.selector.clear()
		}
		return false
	}
	selectedValue := ""
	if m.selector.kind == selectionPath {
		if selected, ok := m.selector.selectedItem(); ok {
			selectedValue = selected.Value
		}
	}
	items := m.matchingPathSuggestions(completion)
	if len(items) == 0 {
		if m.selector.kind == selectionPath {
			m.selector.clear()
		}
		return true
	}
	m.selector = newSelection(selectionPath, items, selectedValue, maxSelectionRows)
	return true
}

func pathCompletionForInput(input string) (pathCompletionContext, bool) {
	if completion, ok := cdPathCompletion(input); ok {
		return completion, true
	}
	return atPathCompletion(input)
}

func cdPathCompletion(input string) (pathCompletionContext, bool) {
	if !strings.HasPrefix(input, "/cd") || len(input) <= len("/cd") || !isPathSpace(rune(input[len("/cd")])) {
		return pathCompletionContext{}, false
	}
	pathStart := len("/cd")
	for pathStart < len(input) && isPathSpace(rune(input[pathStart])) {
		pathStart++
	}
	raw, quoted, ok := trailingPathToken(input[pathStart:])
	if !ok {
		return pathCompletionContext{}, false
	}
	return pathCompletionContext{
		input:           input,
		pathStart:       pathStart,
		rawPath:         raw,
		quoted:          quoted,
		directoriesOnly: true,
	}, true
}

func atPathCompletion(input string) (pathCompletionContext, bool) {
	for index := len(input) - 1; index >= 0; index-- {
		if input[index] != '@' || !pathReferenceBoundary(input, index) {
			continue
		}
		raw, quoted, ok := trailingPathToken(input[index+1:])
		if !ok {
			continue
		}
		return pathCompletionContext{
			input:     input,
			pathStart: index,
			rawPath:   raw,
			marker:    "@",
			quoted:    quoted,
		}, true
	}
	return pathCompletionContext{}, false
}

func trailingPathToken(token string) (raw string, quoted bool, ok bool) {
	if token == "" {
		return "", false, true
	}
	if token[0] == '"' || token[0] == '\'' {
		quote := token[0]
		if strings.ContainsRune(token[1:], rune(quote)) {
			return "", false, false
		}
		return token[1:], true, true
	}
	if strings.IndexFunc(token, isPathSpace) >= 0 {
		return "", false, false
	}
	return token, false, true
}

func pathReferenceBoundary(input string, index int) bool {
	if index == 0 {
		return true
	}
	previous, _ := utf8.DecodeLastRuneInString(input[:index])
	return unicode.IsSpace(previous) || strings.ContainsRune("([{", previous)
}

func isPathSpace(value rune) bool {
	return unicode.IsSpace(value)
}

func (m model) matchingPathSuggestions(completion pathCompletionContext) []selectionItem {
	if m.workspaceRoot == "" || m.cwd == "" {
		return nil
	}
	directoryPart, namePrefix := filepath.Split(filepath.FromSlash(completion.rawPath))
	directoryRequest := directoryPart
	if directoryRequest == "" {
		directoryRequest = "."
	}
	_, directory, err := resolveWorkspacePath(m.workspaceRoot, m.cwd, directoryRequest)
	if err != nil {
		return nil
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil
	}
	sort.SliceStable(entries, func(i, j int) bool {
		if entries[i].IsDir() != entries[j].IsDir() {
			return entries[i].IsDir()
		}
		return strings.ToLower(entries[i].Name()) < strings.ToLower(entries[j].Name())
	})
	items := make([]selectionItem, 0, min(len(entries), maxPathSuggestions))
	for _, entry := range entries {
		if !strings.HasPrefix(strings.ToLower(entry.Name()), strings.ToLower(namePrefix)) {
			continue
		}
		if completion.directoriesOnly && !entry.IsDir() {
			continue
		}
		candidatePath := filepath.ToSlash(filepath.Join(directoryPart, entry.Name()))
		description := "file"
		if entry.IsDir() {
			candidatePath += "/"
			description = "directory"
		} else if isImageExtension(candidatePath) {
			description = "image"
		}
		value := completedPathValue(completion, candidatePath, entry.IsDir())
		items = append(items, selectionItem{
			Value:       value,
			Label:       completion.marker + candidatePath,
			Description: description,
		})
		if len(items) == maxPathSuggestions {
			break
		}
	}
	return items
}

func completedPathValue(completion pathCompletionContext, candidatePath string, directory bool) string {
	quoted := completion.quoted || strings.IndexFunc(candidatePath, isPathSpace) >= 0
	token := completion.marker
	if quoted {
		token += `"` + candidatePath
		if !directory {
			token += `"`
		}
	} else {
		token += candidatePath
	}
	if !directory {
		token += " "
	}
	return completion.input[:completion.pathStart] + token
}

func isImageExtension(path string) bool {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".avif", ".bmp", ".gif", ".jpeg", ".jpg", ".png", ".webp":
		return true
	default:
		return false
	}
}

func (m model) pathInputHasExactTarget() bool {
	completion, ok := pathCompletionForInput(m.composer.Value())
	if !ok || strings.TrimSpace(completion.rawPath) == "" {
		return false
	}
	_, _, err := resolveWorkspacePath(m.workspaceRoot, m.cwd, completion.rawPath)
	return err == nil
}
