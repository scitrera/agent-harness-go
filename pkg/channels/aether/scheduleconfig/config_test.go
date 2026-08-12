package scheduleconfig

import (
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"
)

func TestLoadIsStrictAndCredentialFree(t *testing.T) {
	for name, body := range map[string]string{
		"unknown field":     "version: 1\nschedules:\n  - id: review\n    bearer_token: secret\n",
		"trailing document": "version: 1\nschedules: []\n---\nversion: 1\nschedules: []\n",
		"unknown version":   "version: 2\nschedules: []\n",
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "schedules.yaml")
			if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, _, err := Load(path); err == nil {
				t.Fatal("invalid schedule configuration was accepted")
			}
		})
	}
}

func TestLoadPreservesAuthorityPolicy(t *testing.T) {
	path := filepath.Join(t.TempDir(), "schedules.yaml")
	body := `version: 1
schedules:
  - id: review
    schedule: {type: cron, expression: "0 3 * * *"}
    prompt: Review the workspace.
    require_task_authority: true
    required_downstream_authority_hops: 1
`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	declarations, digest, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(declarations) != 1 || !declarations[0].RequireTaskAuthority ||
		declarations[0].RequiredDownstreamAuthorityHops != 1 || digest == [sha256.Size]byte{} {
		t.Fatalf("declarations=%+v digest=%x", declarations, digest)
	}
}
