// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadInvocationAuthorityPolicy(t *testing.T) {
	path := filepath.Join(t.TempDir(), "authority.json")
	raw := `{
  "profiles": [{
    "ref": {
      "provider_id": "provider", "registration_id": "registration",
      "generation": "generation", "name": "vfs_search", "revision": "sha256:v1"
    },
    "profile": {
      "mode": "caller_obo",
      "resource_scope": [{"resource_type": "vfs", "patterns": ["workspaces/project-a/*"]}],
      "operation_scope": ["read"], "max_access_level": 10
    }
  }]
}`
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	policy, digest, err := loadInvocationAuthorityPolicy(path)
	if err != nil {
		t.Fatal(err)
	}
	if policy == nil || len(digest) != 16 || digest == "none" {
		t.Fatalf("loaded policy = %T digest=%q", policy, digest)
	}
	if _, _, err := loadInvocationAuthorityPolicy(""); err != nil {
		t.Fatal(err)
	}

	badPath := filepath.Join(t.TempDir(), "bad.json")
	if err := os.WriteFile(badPath, []byte(`{"profiles":[],"unknown":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := loadInvocationAuthorityPolicy(badPath); err == nil || !strings.Contains(err.Error(), "unknown") {
		t.Fatalf("unknown policy field error = %v", err)
	}
}
