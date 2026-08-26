// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package providerauth

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	llmauth "github.com/scitrera/go-llm/auth"
)

func TestFileStoreRoundTripUsesPrivatePermissions(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "auth")
	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	want := llmauth.Credential{Provider: KindOpenAISubscription, AccessToken: "secret", RefreshToken: "refresh", Email: "person@example.test"}
	if err := store.Save(context.Background(), "personal", want); err != nil {
		t.Fatal(err)
	}
	got, err := store.Load(context.Background(), "personal")
	if err != nil || got.AccessToken != want.AccessToken || got.RefreshToken != want.RefreshToken {
		t.Fatalf("Load() = %#v, %v", got, err)
	}
	if info, err := os.Stat(dir); err != nil || info.Mode().Perm() != 0o700 {
		t.Fatalf("directory mode = %v, err = %v", info.Mode().Perm(), err)
	}
	path := filepath.Join(dir, "personal.json")
	if info, err := os.Stat(path); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("credential mode = %v, err = %v", info.Mode().Perm(), err)
	}
	if err := store.Delete(context.Background(), "personal"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Load(context.Background(), "personal"); !errors.Is(err, llmauth.ErrCredentialNotFound) {
		t.Fatalf("Load() after delete error = %v", err)
	}
}

func TestFileStoreRejectsPathTraversal(t *testing.T) {
	store, _ := NewFileStore(t.TempDir())
	if err := store.Save(context.Background(), "../escape", llmauth.Credential{}); err == nil {
		t.Fatal("Save() accepted path traversal profile")
	}
}
