// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package localtools

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestWorkspaceApplyPatchAppliesMultipleFilesAtomically(t *testing.T) {
	ctx := context.Background()
	ws := newTestWorkspace(t)
	if err := ws.WriteFile(ctx, "update.txt", "alpha\nbeta\n"); err != nil {
		t.Fatal(err)
	}
	if err := ws.WriteFile(ctx, "delete.txt", "gone\n"); err != nil {
		t.Fatal(err)
	}
	patch := `*** Begin Patch
*** Add File: created.txt
+created
*** Update File: update.txt
@@
-beta
+bravo
*** Delete File: delete.txt
*** End Patch`
	operations, err := ParsePatch(patch)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	changes, err := ws.ApplyPatch(ctx, operations)

	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if len(changes) != 3 {
		t.Fatalf("changes = %#v", changes)
	}
	assertFileContent(t, filepath.Join(ws.root, "created.txt"), "created\n")
	assertFileContent(t, filepath.Join(ws.root, "update.txt"), "alpha\nbravo\n")
	if _, err := os.Stat(filepath.Join(ws.root, "delete.txt")); !os.IsNotExist(err) {
		t.Fatalf("delete.txt still exists: %v", err)
	}
}

func TestWorkspaceApplyPatchValidatesAllOperationsBeforeWriting(t *testing.T) {
	ctx := context.Background()
	ws := newTestWorkspace(t)
	operations, err := ParsePatch(`*** Begin Patch
*** Add File: should-not-exist.txt
+new
*** Update File: missing.txt
@@
-old
+new
*** End Patch`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = ws.ApplyPatch(ctx, operations)

	if !errors.Is(err, ErrInvalidPatch) {
		t.Fatalf("expected invalid patch, got %v", err)
	}
	if _, statErr := os.Stat(filepath.Join(ws.root, "should-not-exist.txt")); !os.IsNotExist(statErr) {
		t.Fatalf("first operation committed despite later failure: %v", statErr)
	}
}

func TestWorkspaceApplyPatchMovesAndPreservesCRLF(t *testing.T) {
	ctx := context.Background()
	ws := newTestWorkspace(t)
	if err := os.WriteFile(filepath.Join(ws.root, "old.txt"), []byte("one\r\ntwo\r\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	operations, err := ParsePatch(`*** Begin Patch
*** Update File: old.txt
*** Move to: nested/new.txt
@@
-two
+second
*** End Patch`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = ws.ApplyPatch(ctx, operations)

	if err != nil {
		t.Fatal(err)
	}
	if _, statErr := os.Stat(filepath.Join(ws.root, "old.txt")); !os.IsNotExist(statErr) {
		t.Fatalf("old path still exists: %v", statErr)
	}
	assertFileContent(t, filepath.Join(ws.root, "nested", "new.txt"), "one\r\nsecond\r\n")
}

func TestWorkspaceApplyPatchRequiresExplicitGrantForExternalPath(t *testing.T) {
	ctx := context.Background()
	ws := newTestWorkspace(t)
	external := t.TempDir()
	path := filepath.Join(external, "new.txt")
	ops := []PatchOperation{{Type: PatchCreate, Path: path, Content: "authorized\n"}}

	if _, err := ws.ApplyPatch(ctx, ops); !errors.Is(err, ErrPathOutsideRoot) {
		t.Fatalf("ungranted external patch = %v", err)
	}
	if err := ws.GrantWorkspaceDirectory(external); err != nil {
		t.Fatal(err)
	}
	if _, err := ws.ApplyPatch(ctx, ops); err != nil {
		t.Fatalf("explicitly granted patch: %v", err)
	}
	assertFileContent(t, path, "authorized\n")
}

func TestParseNativePatchOperation(t *testing.T) {
	ops, err := ParseNativePatchOperation(NativePatchOperation{
		Type: "update_file",
		Path: "file.txt",
		Diff: "@@\n-old\n+new",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(ops) != 1 || ops[0].Type != PatchUpdate || ops[0].Path != "file.txt" {
		t.Fatalf("unexpected operations: %#v", ops)
	}
}

func TestWorkspaceApplyNativeCreateOperation(t *testing.T) {
	ws := newTestWorkspace(t)
	ops, err := ParseNativePatchOperation(NativePatchOperation{
		Type: "create_file",
		Path: "native.txt",
		Diff: "+first\n+second",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ws.ApplyPatch(context.Background(), ops); err != nil {
		t.Fatal(err)
	}
	assertFileContent(t, filepath.Join(ws.root, "native.txt"), "first\nsecond\n")
}

func assertFileContent(t *testing.T, path, want string) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != want {
		t.Fatalf("%s = %q, want %q", path, got, want)
	}
}
