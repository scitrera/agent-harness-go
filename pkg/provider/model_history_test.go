// SPDX-License-Identifier: Apache-2.0
package provider

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
	llmprotocol "github.com/scitrera/go-llm/protocol"
)

func TestSharedProtocolRequestExpandsPersistedToolCycles(t *testing.T) {
	var source protocol.ChatMessage
	if err := json.Unmarshal([]byte(`{"id":"history","role":"assistant","content":[
  {"type":"text","text":"working"},
  {"type":"tool_call","id":"a","name":"first","args":{}},
  {"type":"tool_result","call_id":"a","name":"first","result":"one"},
  {"type":"tool_call","id":"b","name":"second","args":{}},
  {"type":"tool_result","call_id":"b","name":"second","result":"two"},
  {"type":"text","text":"finished"}
 ]}`), &source); err != nil {
		t.Fatal(err)
	}
	before, _ := json.Marshal(source)
	request, err := sharedProtocolRequest(ChatRequest{Messages: []protocol.ChatMessage{source}})
	if err != nil {
		t.Fatal(err)
	}
	roles := []llmprotocol.Role{}
	resultIDs := []string{}
	for _, message := range request.Messages {
		roles = append(roles, message.Role)
		for _, part := range message.Content {
			if part.Result != nil {
				resultIDs = append(resultIDs, part.Result.ToolCallID)
			}
		}
	}
	if !reflect.DeepEqual(roles, []llmprotocol.Role{llmprotocol.RoleAssistant, llmprotocol.RoleTool, llmprotocol.RoleAssistant, llmprotocol.RoleTool, llmprotocol.RoleAssistant}) {
		t.Fatalf("roles=%v", roles)
	}
	if !reflect.DeepEqual(resultIDs, []string{"a", "b"}) {
		t.Fatalf("result ids=%v", resultIDs)
	}
	after, _ := json.Marshal(source)
	if string(before) != string(after) {
		t.Fatal("persisted input was mutated")
	}
}

func TestSharedProtocolRequestSplitsParallelToolResults(t *testing.T) {
	var source protocol.ChatMessage
	if err := json.Unmarshal([]byte(`{"id":"parallel","role":"tool_result","content":[
  {"type":"tool_result","call_id":"a","result":"one"},
  {"type":"tool_result","call_id":"b","result":"two"}
 ]}`), &source); err != nil {
		t.Fatal(err)
	}
	request, err := sharedProtocolRequest(ChatRequest{Messages: []protocol.ChatMessage{source}})
	if err != nil {
		t.Fatal(err)
	}
	if len(request.Messages) != 2 {
		t.Fatalf("messages=%d", len(request.Messages))
	}
	for _, m := range request.Messages {
		if m.Role != llmprotocol.RoleTool || len(m.Content) != 1 {
			t.Fatalf("invalid tool message: %+v", m)
		}
	}
}
