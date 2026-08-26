package subagent

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestOutputContractValidatesObjectAndProducesDigest(t *testing.T) {
	contract, err := CompileOutputSchema(json.RawMessage(`{
		"type":"object","required":["status","findings"],"additionalProperties":false,
		"properties":{"status":{"type":"string","enum":["ok","needs_work"]},"findings":{"type":"array","items":{"type":"string"}}}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if contract.Digest() == "" || len(contract.Raw()) == 0 {
		t.Fatalf("contract = %+v", contract)
	}
	if errors := contract.Validate(json.RawMessage(`{"status":"ok","findings":[]}`)); len(errors) != 0 {
		t.Fatalf("valid output errors = %+v", errors)
	}
	errors := contract.Validate(json.RawMessage(`{"status":"other","extra":true}`))
	joined, _ := json.Marshal(errors)
	if len(errors) < 3 || !strings.Contains(string(joined), "findings") || !strings.Contains(string(joined), "enum") || !strings.Contains(string(joined), "additional") {
		t.Fatalf("validation errors = %s", joined)
	}
}

func TestOutputContractRejectsUnsupportedKeyword(t *testing.T) {
	if _, err := CompileOutputSchema(json.RawMessage(`{"type":"object","oneOf":[]}`)); err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("unsupported keyword error = %v", err)
	}
}

func TestOutputContractRejectsMalformedOrTrailingSchema(t *testing.T) {
	for _, raw := range []string{
		`{"type":"string","enum":"not-an-array"}`,
		`{"type":"string"} {"type":"number"}`,
		`{"type":"string"} trailing`,
	} {
		if _, err := CompileOutputSchema(json.RawMessage(raw)); err == nil {
			t.Fatalf("CompileOutputSchema(%q) succeeded", raw)
		}
	}
}
