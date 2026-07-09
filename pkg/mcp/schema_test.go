package mcp

import (
	"encoding/json"
	"testing"
)

func Test_toolArgSchema_dropEmptyOptionalStrings(t *testing.T) {
	// Given
	schema := parseToolArgSchema(json.RawMessage(`{
		"type": "object",
		"required": ["name"],
		"properties": {
			"name": {"type": "string"},
			"note": {"type": "string"},
			"label": {"type": "string"},
			"count": {"type": "number"}
		}
	}`))
	args := json.RawMessage(`{"name":"","note":"","label":"hi","count":0}`)

	// When
	normalized := schema.dropEmptyOptionalStrings(args)

	// Then
	var decoded map[string]json.RawMessage
	if err := json.Unmarshal(normalized, &decoded); err != nil {
		t.Fatalf("decode normalized args: %v", err)
	}
	if _, ok := decoded["note"]; ok {
		t.Fatalf("expected empty optional string field to be dropped, got %#v", decoded)
	}
	if str, ok := decoded["name"]; !ok || string(str) != `""` {
		t.Fatalf("expected required empty string field to be preserved, got %#v", decoded)
	}
	if str, ok := decoded["label"]; !ok || string(str) != `"hi"` {
		t.Fatalf("expected non-empty optional string field to be preserved, got %#v", decoded)
	}
	if str, ok := decoded["count"]; !ok || string(str) != "0" {
		t.Fatalf("expected non-string field to be untouched, got %#v", decoded)
	}
}

func Test_toolArgSchema_dropEmptyOptionalStrings_leaves_non_object_args_unmodified(t *testing.T) {
	// Given
	schema := parseToolArgSchema(json.RawMessage(`{"type":"object","properties":{"note":{"type":"string"}}}`))
	args := json.RawMessage(`[]`)

	// When
	normalized := schema.dropEmptyOptionalStrings(args)

	// Then
	if string(normalized) != string(args) {
		t.Fatalf("expected non-object args to be left untouched, got %s", normalized)
	}
}
