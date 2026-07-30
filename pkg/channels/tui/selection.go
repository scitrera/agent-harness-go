package tui

import "strings"

const maxSelectionRows = 6

type selectionKind int

const (
	selectionNone selectionKind = iota
	selectionSlash
	selectionPath
	selectionThread
	selectionApproval
	selectionTool
)

func (k selectionKind) String() string {
	switch k {
	case selectionSlash:
		return "command"
	case selectionPath:
		return "path"
	case selectionThread:
		return "thread"
	case selectionApproval:
		return "approval"
	case selectionTool:
		return "tool"
	default:
		return "item"
	}
}

type selectionItem struct {
	Value       string
	Label       string
	Description string
}

type selectionState struct {
	kind         selectionKind
	allItems     []selectionItem
	items        []selectionItem
	selected     int
	visibleLimit int
	filtering    bool
	query        string
}

func newSelection(kind selectionKind, items []selectionItem, selectedValue string, visibleLimit int) selectionState {
	if len(items) == 0 {
		return selectionState{}
	}
	if visibleLimit <= 0 {
		visibleLimit = maxSelectionRows
	}
	selection := selectionState{
		kind:         kind,
		allItems:     append([]selectionItem(nil), items...),
		items:        append([]selectionItem(nil), items...),
		visibleLimit: visibleLimit,
	}
	for i, item := range selection.items {
		if item.Value == selectedValue {
			selection.selected = i
			return selection
		}
	}
	return selection
}

func (s selectionState) active() bool {
	return s.kind != selectionNone && (len(s.items) > 0 || s.filtering)
}

func (s selectionState) height() int {
	if !s.active() {
		return 0
	}
	height := s.itemHeight()
	if s.filtering || s.query != "" {
		height++
	}
	if len(s.items) == 0 {
		height++
	}
	return height
}

func (s selectionState) itemHeight() int {
	if s.visibleLimit <= 0 || len(s.items) < s.visibleLimit {
		return len(s.items)
	}
	return s.visibleLimit
}

func (s *selectionState) limitHeight(available int) {
	if s.kind == selectionNone {
		return
	}
	overhead := 0
	if s.filtering || s.query != "" {
		overhead++
	}
	if len(s.items) == 0 {
		overhead++
	}
	itemLimit := available - overhead
	if itemLimit < 1 {
		itemLimit = 1
	}
	if itemLimit > maxSelectionRows {
		itemLimit = maxSelectionRows
	}
	s.visibleLimit = itemLimit
}

func (s selectionState) selectedItem() (selectionItem, bool) {
	if !s.active() || s.selected < 0 || s.selected >= len(s.items) {
		return selectionItem{}, false
	}
	return s.items[s.selected], true
}

func (s selectionState) visibleItems() (int, []selectionItem) {
	height := s.itemHeight()
	if height == 0 {
		return 0, nil
	}
	start := s.selected - height/2
	if start < 0 {
		start = 0
	}
	if start+height > len(s.items) {
		start = len(s.items) - height
	}
	return start, s.items[start : start+height]
}

func (s *selectionState) move(delta int) {
	if len(s.items) == 0 {
		return
	}
	s.selected = (s.selected + delta + len(s.items)) % len(s.items)
}

func (s selectionState) searchable() bool {
	switch s.kind {
	case selectionThread, selectionApproval, selectionTool:
		return true
	default:
		return false
	}
}

func (s *selectionState) beginFilter() {
	if !s.searchable() {
		return
	}
	s.filtering = true
	s.applyFilter("")
}

func (s *selectionState) appendFilter(text string) {
	s.query += text
	s.applyFilter("")
}

func (s *selectionState) backspaceFilter() {
	runes := []rune(s.query)
	if len(runes) == 0 {
		return
	}
	s.query = string(runes[:len(runes)-1])
	s.applyFilter("")
}

func (s *selectionState) clearFilter() {
	s.query = ""
	s.filtering = false
	s.applyFilter("")
}

func (s *selectionState) replaceItems(items []selectionItem, selectedValue string) {
	s.allItems = append([]selectionItem(nil), items...)
	s.applyFilter(selectedValue)
}

func (s *selectionState) applyFilter(selectedValue string) {
	if selectedValue == "" {
		if selected, ok := s.selectedItem(); ok {
			selectedValue = selected.Value
		}
	}
	query := strings.ToLower(strings.TrimSpace(s.query))
	s.items = s.items[:0]
	for _, item := range s.allItems {
		searchable := strings.ToLower(item.Value + " " + item.Label + " " + item.Description)
		if query == "" || strings.Contains(searchable, query) {
			s.items = append(s.items, item)
		}
	}
	s.selected = 0
	for i, item := range s.items {
		if item.Value == selectedValue {
			s.selected = i
			break
		}
	}
}

func (s *selectionState) clear() {
	*s = selectionState{}
}
