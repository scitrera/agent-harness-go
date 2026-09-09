// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package compaction

import (
	"context"
	"testing"
)

func TestWorldStateCompactionsMergesMax(t *testing.T) {
	m := MergeWorldState(
		WorldState{Compactions: 2},
		WorldState{Compactions: 5},
		WorldState{Compactions: 3},
	)
	if m.Compactions != 5 {
		t.Fatalf("Compactions merge = %d, want 5 (max, monotonic like Turn)", m.Compactions)
	}
}

func TestNoteCompactionBumpsCtxCounter(t *testing.T) {
	ctx, c := WithCompactionCounter(context.Background())
	NoteCompaction(ctx)
	NoteCompaction(ctx)
	if *c != 2 {
		t.Fatalf("counter = %d, want 2", *c)
	}
	// No counter on ctx → no-op, no panic.
	NoteCompaction(context.Background())
}
