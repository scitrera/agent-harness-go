package provider

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

// DeltaFunc receives streamed text tokens as they arrive.
type DeltaFunc func(text string) error

// ChatStream issues a streaming chat request. Text tokens are delivered to
// onDelta as they arrive; the accumulated final message (text + tool calls) is
// returned. If the endpoint does not respond with SSE, it falls back to
// decoding a normal JSON response (emitting the full text as one delta).
func (c *SidecarClient) ChatStream(ctx context.Context, chat ChatRequest, onDelta DeltaFunc) (ChatResponse, error) {
	chat.Stream = true
	chat.Messages = sanitizeTranscript(chat.Messages)
	body, err := c.encodeRequest(chat)
	if err != nil {
		return ChatResponse{}, fmt.Errorf("marshal chat request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint(), bytes.NewReader(body))
	if err != nil {
		return ChatResponse{}, fmt.Errorf("create chat request: %w", err)
	}
	req.Header.Set("content-type", "application/json")
	req.Header.Set("accept", "text/event-stream")
	if c.authHeader != "" {
		req.Header.Set("authorization", c.authHeader)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return ChatResponse{}, classifyTransport(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
		return ChatResponse{}, httpError(resp.StatusCode, string(data))
	}
	if !strings.Contains(resp.Header.Get("content-type"), "text/event-stream") {
		data, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
		if err != nil {
			return ChatResponse{}, fmt.Errorf("read chat response: %w", err)
		}
		out, err := decodeChatResponse(data)
		if err != nil {
			return ChatResponse{}, err
		}
		for _, part := range out.Message.Content {
			if tp, ok := part.AsText(); ok && tp.Text != "" {
				if err := onDelta(tp.Text); err != nil {
					return ChatResponse{}, err
				}
			}
		}
		return out, nil
	}
	return parseSSEStream(resp.Body, onDelta)
}

type sseToolAccumulator struct {
	id   string
	name string
	args strings.Builder
}

func parseSSEStream(r io.Reader, onDelta DeltaFunc) (ChatResponse, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 8<<20)

	var id string
	role := protocol.RoleAssistant
	var text strings.Builder
	tools := map[int]*sseToolAccumulator{}
	var order []int

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(line, "data:") {
			continue // skip blank lines, comments, event: lines
		}
		payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if payload == "[DONE]" {
			break
		}
		var chunk struct {
			ID      string `json:"id"`
			Choices []struct {
				Delta struct {
					Role      protocol.Role `json:"role"`
					Content   string        `json:"content"`
					ToolCalls []struct {
						Index    int    `json:"index"`
						ID       string `json:"id"`
						Function struct {
							Name      string `json:"name"`
							Arguments string `json:"arguments"`
						} `json:"function"`
					} `json:"tool_calls"`
				} `json:"delta"`
			} `json:"choices"`
		}
		if err := json.Unmarshal([]byte(payload), &chunk); err != nil {
			continue // tolerate keepalives / non-JSON frames
		}
		if chunk.ID != "" {
			id = chunk.ID
		}
		if len(chunk.Choices) == 0 {
			continue
		}
		delta := chunk.Choices[0].Delta
		if delta.Role != "" {
			role = delta.Role
		}
		if delta.Content != "" {
			text.WriteString(delta.Content)
			if err := onDelta(delta.Content); err != nil {
				return ChatResponse{}, err
			}
		}
		for _, tc := range delta.ToolCalls {
			acc := tools[tc.Index]
			if acc == nil {
				acc = &sseToolAccumulator{}
				tools[tc.Index] = acc
				order = append(order, tc.Index)
			}
			if tc.ID != "" {
				acc.id = tc.ID
			}
			if tc.Function.Name != "" {
				acc.name = tc.Function.Name
			}
			acc.args.WriteString(tc.Function.Arguments)
		}
	}
	if err := scanner.Err(); err != nil {
		return ChatResponse{}, fmt.Errorf("read stream: %w", err)
	}

	parts := make([]protocol.ContentPart, 0, len(order)+1)
	if text.Len() > 0 {
		part, err := protocol.NewTextPart(text.String())
		if err != nil {
			return ChatResponse{}, err
		}
		parts = append(parts, part)
	}
	sort.Ints(order)
	for i, idx := range order {
		acc := tools[idx]
		callID := acc.id
		if callID == "" {
			callID = fmt.Sprintf("call-%d", i)
		}
		part, err := protocol.NewToolCallPart(protocol.ToolInvokeEnvelope{
			CallID: callID,
			Name:   acc.name,
			Args:   toolCallArgs(json.RawMessage(acc.args.String())),
		})
		if err != nil {
			return ChatResponse{}, err
		}
		parts = append(parts, part)
	}
	if len(parts) == 0 {
		part, err := protocol.NewTextPart("")
		if err != nil {
			return ChatResponse{}, err
		}
		parts = append(parts, part)
	}
	return ChatResponse{Message: protocol.ChatMessage{ID: id, Role: role, Content: parts}}, nil
}
