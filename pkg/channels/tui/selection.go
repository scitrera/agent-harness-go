package tui

const maxSelectionRows = 3

type selectionKind int

const (
	selectionNone selectionKind = iota
	selectionSlash
	selectionThread
)

type selectionItem struct {
	Value       string
	Label       string
	Description string
}

type selectionState struct {
	kind         selectionKind
	items        []selectionItem
	selected     int
	visibleLimit int
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
	return s.kind != selectionNone && len(s.items) > 0
}

func (s selectionState) height() int {
	if !s.active() {
		return 0
	}
	if s.visibleLimit <= 0 || len(s.items) < s.visibleLimit {
		return len(s.items)
	}
	return s.visibleLimit
}

func (s selectionState) selectedItem() (selectionItem, bool) {
	if !s.active() || s.selected < 0 || s.selected >= len(s.items) {
		return selectionItem{}, false
	}
	return s.items[s.selected], true
}

func (s selectionState) visibleItems() (int, []selectionItem) {
	height := s.height()
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
	if !s.active() {
		s.clear()
		return
	}
	s.selected = (s.selected + delta + len(s.items)) % len(s.items)
}

func (s *selectionState) clear() {
	*s = selectionState{}
}
