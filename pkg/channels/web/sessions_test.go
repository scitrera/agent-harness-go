package web

import (
	"testing"
	"time"
)

func fixedClock() func() time.Time {
	var n int64
	return func() time.Time { n++; return time.Unix(n, 0) }
}

func TestSessionsCreateListDelete(t *testing.T) {
	dir := t.TempDir()
	x, err := NewIndex(dir, fixedClock())
	if err != nil {
		t.Fatalf("new index: %v", err)
	}
	a, err := x.Create()
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	b, err := x.Create()
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	list := x.List()
	if len(list) != 2 {
		t.Fatalf("expected 2 sessions, got %d", len(list))
	}
	// Newest-updated first: b was created after a.
	if list[0].ID != b.ID {
		t.Fatalf("expected newest first; got %s then %s", list[0].ID, list[1].ID)
	}
	if err := x.Delete(a.ID); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if len(x.List()) != 1 {
		t.Fatalf("expected 1 after delete")
	}
}

func TestSessionsTouchSetsTitleOnce(t *testing.T) {
	dir := t.TempDir()
	x, err := NewIndex(dir, fixedClock())
	if err != nil {
		t.Fatalf("new index: %v", err)
	}
	s, _ := x.Create()
	if err := x.Touch(s.ID, "first question about taxes"); err != nil {
		t.Fatalf("touch: %v", err)
	}
	if got := x.List()[0].Title; got != "first question about taxes" {
		t.Fatalf("title not set from first message: %q", got)
	}
	// A second message must not overwrite the established title.
	if err := x.Touch(s.ID, "a different follow-up"); err != nil {
		t.Fatalf("touch: %v", err)
	}
	if got := x.List()[0].Title; got != "first question about taxes" {
		t.Fatalf("title should be stable, got %q", got)
	}
}

func TestSessionsPersistAcrossReload(t *testing.T) {
	dir := t.TempDir()
	x, _ := NewIndex(dir, fixedClock())
	s, _ := x.Create()
	_ = x.Touch(s.ID, "remember me")

	reloaded, err := NewIndex(dir, fixedClock())
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	list := reloaded.List()
	if len(list) != 1 || list[0].Title != "remember me" {
		t.Fatalf("index did not persist: %+v", list)
	}
}
