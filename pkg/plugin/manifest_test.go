// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package plugin

import (
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"
)

func testDigest(value string) string {
	return fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(value)))
}

func TestDecodeAndAdmitManifestIsAdditiveAndPolicyBounded(t *testing.T) {
	digest := testDigest("artifact")
	manifest, err := DecodeManifest([]byte(fmt.Sprintf(`{
		"schema_version":1,"id":"com.example/reviewer","revision":"1.2.3",
		"artifact_digest":%q,"requested_capabilities":["filesystem.read"],
		"tools":[{"name":"review_code","input_schema":{"type":"object"}}],
		"future_optional_field":{"ignored":true}
	}`, digest)))
	if err != nil {
		t.Fatal(err)
	}
	admission, err := Admit(manifest, Policy{
		ArtifactDigest:      digest,
		GrantedCapabilities: map[Capability]struct{}{CapabilityFilesystemRead: {}},
		CoreToolNames:       map[string]struct{}{"read_file": {}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if admission.Manifest.ID != "com.example/reviewer" || len(admission.GrantedCapabilities) != 1 {
		t.Fatalf("admission = %+v", admission)
	}
}

func TestAdmissionRejectsSelfElevationDigestAndCoreCollision(t *testing.T) {
	digest := testDigest("artifact")
	base := Manifest{SchemaVersion: 1, ID: "com.example/plugin", Revision: "1", ArtifactDigest: digest}
	policy := Policy{ArtifactDigest: digest, CoreToolNames: map[string]struct{}{"read_file": {}}}

	selfAuthorized := base
	selfAuthorized.Preauthorized = true
	if _, err := Admit(selfAuthorized, policy); err == nil || !strings.Contains(err.Error(), "self-preauthorize") {
		t.Fatalf("self-preauthorization error = %v", err)
	}
	wrongDigest := base
	wrongDigest.ArtifactDigest = testDigest("other")
	if _, err := Admit(wrongDigest, policy); err == nil || !strings.Contains(err.Error(), "digest") {
		t.Fatalf("digest error = %v", err)
	}
	coreCollision := base
	coreCollision.Tools = []Tool{{Name: "read_file"}}
	if _, err := Admit(coreCollision, policy); err == nil || !strings.Contains(err.Error(), "core tool") {
		t.Fatalf("core collision error = %v", err)
	}
	ungranted := base
	ungranted.RequestedCapabilities = []Capability{CapabilityNetwork}
	if _, err := Admit(ungranted, policy); err == nil || !strings.Contains(err.Error(), "not granted") {
		t.Fatalf("grant ceiling error = %v", err)
	}
}

func TestRegistryRejectsPluginAndToolCollisions(t *testing.T) {
	digest := testDigest("one")
	first, err := Admit(Manifest{SchemaVersion: 1, ID: "com.example/one", Revision: "1", ArtifactDigest: digest, Tools: []Tool{{Name: "review_code"}}}, Policy{ArtifactDigest: digest})
	if err != nil {
		t.Fatal(err)
	}
	registry := NewRegistry()
	if err := registry.Register(first); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(first); err != nil {
		t.Fatalf("idempotent registration: %v", err)
	}
	otherDigest := testDigest("two")
	other, err := Admit(Manifest{SchemaVersion: 1, ID: "com.example/two", Revision: "1", ArtifactDigest: otherDigest, Tools: []Tool{{Name: "review_code"}}}, Policy{ArtifactDigest: otherDigest})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(other); err == nil || !strings.Contains(err.Error(), "already belongs") {
		t.Fatalf("tool collision error = %v", err)
	}
}
