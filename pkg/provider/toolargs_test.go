// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package provider

import (
	"encoding/json"
	"testing"
)

// TestToolCallArgs_UnwrapsDollarText reproduces the real MLflow-gateway / fireworks
// artifact (captured from ml-postgres) where each array item of a tool call's
// arguments arrives wrapped as {"$text":"<the item as a JSON string>"} instead of
// the object, which made every todo item decode to empty content.
func TestToolCallArgs_UnwrapsDollarText(t *testing.T) {
	inner := `{ "id": "1", "content": "Extract PDF text", "status": "in_progress" }`
	argsObj := map[string]any{"items": []any{map[string]any{"$text": inner}}}
	argsJSON, _ := json.Marshal(argsObj)
	raw, _ := json.Marshal(string(argsJSON)) // OpenAI tool-call: arguments is a JSON string

	got := toolCallArgs(raw)
	var items []map[string]any
	if err := json.Unmarshal(got["items"], &items); err != nil {
		t.Fatalf("items decode: %v (got=%s)", err, got["items"])
	}
	if len(items) != 1 {
		t.Fatalf("want 1 item, got %d", len(items))
	}
	if items[0]["id"] != "1" || items[0]["content"] != "Extract PDF text" || items[0]["status"] != "in_progress" {
		t.Fatalf("item not unwrapped: %#v", items[0])
	}
}

func TestToolCallArgs_PassesThroughWellFormed(t *testing.T) {
	raw, _ := json.Marshal(`{"items":[{"id":"1","content":"do X","status":"pending"}]}`)
	got := toolCallArgs(raw)
	var items []map[string]any
	if err := json.Unmarshal(got["items"], &items); err != nil {
		t.Fatalf("items decode: %v", err)
	}
	if len(items) != 1 || items[0]["content"] != "do X" {
		t.Fatalf("well-formed args mangled: %s", got["items"])
	}
}

func TestRepairDollarText_LeavesNonStructuredScalars(t *testing.T) {
	// "hello" is not JSON -> not unwrapped.
	if _, changed := repairDollarText(map[string]any{"note": map[string]any{"$text": "hello"}}); changed {
		t.Fatal("must not unwrap a non-JSON $text string")
	}
	// "123" parses as a scalar (number), not an object/array -> not unwrapped.
	if _, changed := repairDollarText(map[string]any{"n": map[string]any{"$text": "123"}}); changed {
		t.Fatal("must not unwrap a scalar-JSON $text string")
	}
}
