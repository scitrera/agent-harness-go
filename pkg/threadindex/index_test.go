// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package threadindex

import (
	"testing"
	"time"
)

func fixedClock() func() time.Time {
	var n int64
	return func() time.Time { n++; return time.Unix(n, 0) }
}

func TestIndexCreateListDelete_whenSessionsChange(t *testing.T) {
	// Given
	index, err := NewIndex(t.TempDir(), fixedClock())
	if err != nil {
		t.Fatalf("new index: %v", err)
	}
	a, err := index.Create()
	if err != nil {
		t.Fatalf("create a: %v", err)
	}
	b, err := index.Create()
	if err != nil {
		t.Fatalf("create b: %v", err)
	}

	// When
	list := index.List()

	// Then
	if len(list) != 2 || list[0].ID != b.ID || list[1].ID != a.ID {
		t.Fatalf("newest first mismatch: %+v", list)
	}
	if err := index.Delete(a.ID); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if got := index.List(); len(got) != 1 || got[0].ID != b.ID {
		t.Fatalf("delete mismatch: %+v", got)
	}
}

func TestIndexTouchTitle_whenFirstMessageArrives(t *testing.T) {
	// Given
	index, err := NewIndex(t.TempDir(), fixedClock())
	if err != nil {
		t.Fatalf("new index: %v", err)
	}
	session, err := index.Create()
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// When
	if err := index.Touch(session.ID, "first question about taxes"); err != nil {
		t.Fatalf("touch: %v", err)
	}
	if err := index.Touch(session.ID, "follow up"); err != nil {
		t.Fatalf("touch again: %v", err)
	}

	// Then
	if got := index.List()[0].Title; got != "first question about taxes" {
		t.Fatalf("title = %q", got)
	}
}

func TestIndexRename_whenTitleExplicit(t *testing.T) {
	// Given
	index, err := NewIndex(t.TempDir(), fixedClock())
	if err != nil {
		t.Fatalf("new index: %v", err)
	}
	session, err := index.Create()
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// When
	if err := index.Rename(session.ID, "Project planning"); err != nil {
		t.Fatalf("rename: %v", err)
	}

	// Then
	if got := index.List()[0].Title; got != "Project planning" {
		t.Fatalf("renamed title = %q", got)
	}
}
