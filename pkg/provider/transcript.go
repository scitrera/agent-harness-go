package provider

import "github.com/scitrera/agent-harness-go/pkg/protocol"

// sanitizeTranscript repairs tool-call/tool-result pairing before a request is
// encoded, preventing provider 400s from a transcript our own context handling
// may have split. OpenAI-compatible providers reject:
//
//   - a tool_result whose call_id has no preceding assistant tool_call (orphan
//     result — e.g. the assistant turn was trimmed by compaction), and
//   - an assistant tool_call with no following tool_result (unanswered call —
//     e.g. the result turn was trimmed).
//
// It drops orphan tool_result parts and strips unanswered tool_call parts, then
// drops any message left with no content. Contentless terminal-marker messages
// are also removed: their metadata is useful to history UIs but has no valid
// OpenAI chat representation. Valid transcripts pass through unchanged (a
// fresh slice is only built when a repair is needed).
func sanitizeTranscript(messages []protocol.ChatMessage) []protocol.ChatMessage {
	answered := map[string]bool{} // tool_call ids that have a matching tool_result
	called := map[string]bool{}   // tool_call ids that were issued
	for _, m := range messages {
		for _, p := range m.Content {
			switch p.Type() {
			case protocol.ContentToolCall:
				if tc, ok := p.AsToolCall(); ok {
					called[tc.ID] = true
				}
			case protocol.ContentToolResult:
				if tr, ok := p.AsToolResult(); ok {
					answered[tr.CallID] = true
				}
			}
		}
	}
	if !needsRepair(messages, answered, called) {
		return messages
	}
	out := make([]protocol.ChatMessage, 0, len(messages))
	for _, m := range messages {
		kept := make([]protocol.ContentPart, 0, len(m.Content))
		for _, p := range m.Content {
			switch p.Type() {
			case protocol.ContentToolCall:
				if tc, ok := p.AsToolCall(); ok && !answered[tc.ID] {
					continue // unanswered call -> strip
				}
			case protocol.ContentToolResult:
				if tr, ok := p.AsToolResult(); ok && !called[tr.CallID] {
					continue // orphan result -> drop
				}
			}
			kept = append(kept, p)
		}
		if len(kept) == 0 {
			continue // message emptied by repair -> drop
		}
		m.Content = kept
		out = append(out, m)
	}
	return out
}

func needsRepair(messages []protocol.ChatMessage, answered, called map[string]bool) bool {
	for _, m := range messages {
		if len(m.Content) == 0 {
			return true
		}
		for _, p := range m.Content {
			switch p.Type() {
			case protocol.ContentToolCall:
				if tc, ok := p.AsToolCall(); ok && !answered[tc.ID] {
					return true
				}
			case protocol.ContentToolResult:
				if tr, ok := p.AsToolResult(); ok && !called[tr.CallID] {
					return true
				}
			}
		}
	}
	return false
}
