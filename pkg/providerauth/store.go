// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package providerauth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sync"

	llmauth "github.com/scitrera/go-llm/auth"
)

var validProfile = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

// FileStore is Sahara OSS's portable credential store. Files and their parent
// directory are private to the current user, and replacement is atomic. A host
// distribution can supply a different llmauth.Store (for example an OS keyring)
// without changing provider code.
type FileStore struct {
	dir string
	mu  sync.RWMutex
}

func NewFileStore(dir string) (*FileStore, error) {
	if dir == "" {
		return nil, fmt.Errorf("provider auth directory is required")
	}
	return &FileStore{dir: filepath.Clean(dir)}, nil
}

func (s *FileStore) Load(_ context.Context, profile string) (llmauth.Credential, error) {
	path, err := s.path(profile)
	if err != nil {
		return llmauth.Credential{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return llmauth.Credential{}, llmauth.ErrCredentialNotFound
	}
	if err != nil {
		return llmauth.Credential{}, fmt.Errorf("read credential profile %q: %w", profile, err)
	}
	var credential llmauth.Credential
	if err := json.Unmarshal(data, &credential); err != nil {
		return llmauth.Credential{}, fmt.Errorf("decode credential profile %q: %w", profile, err)
	}
	return credential, nil
}

func (s *FileStore) Save(_ context.Context, profile string, credential llmauth.Credential) error {
	path, err := s.path(profile)
	if err != nil {
		return err
	}
	data, err := json.Marshal(credential)
	if err != nil {
		return fmt.Errorf("encode credential profile %q: %w", profile, err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := os.MkdirAll(s.dir, 0o700); err != nil {
		return fmt.Errorf("create credential directory: %w", err)
	}
	if err := os.Chmod(s.dir, 0o700); err != nil {
		return fmt.Errorf("protect credential directory: %w", err)
	}
	temporary, err := os.CreateTemp(s.dir, ".credential-*")
	if err != nil {
		return fmt.Errorf("create credential temporary file: %w", err)
	}
	temporaryPath := temporary.Name()
	defer func() { _ = os.Remove(temporaryPath) }()
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("protect credential temporary file: %w", err)
	}
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write credential profile %q: %w", profile, err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("sync credential profile %q: %w", profile, err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close credential profile %q: %w", profile, err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("replace credential profile %q: %w", profile, err)
	}
	return nil
}

func (s *FileStore) Delete(_ context.Context, profile string) error {
	path, err := s.path(profile)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := os.Remove(path); errors.Is(err, os.ErrNotExist) {
		return llmauth.ErrCredentialNotFound
	} else if err != nil {
		return fmt.Errorf("delete credential profile %q: %w", profile, err)
	}
	return nil
}

func (s *FileStore) path(profile string) (string, error) {
	if !validProfile.MatchString(profile) {
		return "", fmt.Errorf("invalid credential profile name %q", profile)
	}
	return filepath.Join(s.dir, profile+".json"), nil
}
