// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
	llmclient "github.com/scitrera/go-llm/client"
	llmprotocol "github.com/scitrera/go-llm/protocol"
)

var (
	ErrInvalidProviderConfig = errors.New("provider: invalid config")
	ErrDirectProviderURL     = errors.New("provider: direct provider url is not allowed in sandbox")
	ErrProviderSecret        = errors.New("provider: provider api key is not allowed in sandbox")
	ErrProviderHTTP          = errors.New("provider: sidecar http request failed")
)

// WireFormat selects how chat requests are encoded.
type WireFormat string

const (
	// FormatNative sends native Scitrera ChatMessages (the sidecar contract).
	FormatNative WireFormat = "native"
	// FormatOpenAI sends OpenAI-style {role, content} messages, for talking to
	// an OpenAI-compatible provider directly (sidecar-less testing).
	FormatOpenAI WireFormat = "openai"
	// FormatResponses uses the OpenAI Responses API through the shared Scitrera
	// protocol/client modules.
	FormatResponses WireFormat = "responses"
)

// Guard validates provider configuration before a client is built; a non-nil
// error rejects construction. Core leaves it nil (permissive). The Scitrera
// distribution wires SandboxGuard to enforce the no-real-secret / no-direct-host
// sandbox policy.
type Guard func(base *url.URL, authHeader string) error

type OpenAICompatConfig struct {
	BaseURL    string
	AuthHeader string
	// Auth supplies dynamic credentials (for example a renewable OAuth profile).
	// It is mutually exclusive with AuthHeader. The host owns credential storage.
	Auth       llmclient.Authenticator
	ChatPath   string
	Format     WireFormat
	HTTPClient *http.Client
	// PrepareRequest applies an optional provider-specific request profile after
	// neutral protocol conversion and before encoding.
	PrepareRequest func(*llmprotocol.Request)
	// StreamFirstChunk bounds the wait for the FIRST streamed chunk (time-to-first-
	// token — long for big-context reasoning). StreamIdle bounds the gap between
	// SUBSEQUENT chunks (once tokens flow, gaps are short). Neither is a total-duration
	// cap. 0 → defaults (see NewOpenAICompatClient).
	StreamFirstChunk time.Duration
	StreamIdle       time.Duration
	// Guard, when set, validates BaseURL + AuthHeader (e.g. SandboxGuard).
	Guard Guard
	// PromptCaching, when true, marks the end of the stable system-prompt prefix
	// with a provider prompt-cache breakpoint on the native wire path (the sidecar
	// lowers it to an Anthropic cache_control:{type:"ephemeral"} block). Opt-in and
	// default false so it cannot regress existing behavior; a no-op on the openai
	// wire format (which drops the hint).
	PromptCaching bool
	// StreamUsage, when true, requests a trailing usage chunk on streamed calls
	// (stream_options.include_usage) and reads it instead of short-circuiting on
	// finish_reason. Opt-in and default false: it must only be enabled against
	// providers that send a terminal `[DONE]`/usage chunk, else the stream waits on
	// the idle-liveness timer. Non-stream calls always report usage regardless.
	StreamUsage bool
}

// OpenAICompatClient talks to an OpenAI-compatible chat endpoint over HTTP. In
// the Scitrera distribution that endpoint is the in-sandbox sidecar (which the
// comments below call "the sidecar"); FormatOpenAI also lets it target any
// OpenAI-compatible provider directly for sidecar-less testing.
type OpenAICompatClient struct {
	baseURL    *url.URL
	chatPath   string
	authHeader string
	format     WireFormat
	client     *http.Client
	// streamClient has NO total timeout (a streaming generation legitimately exceeds
	// one); ChatStream enforces liveness via first-chunk + inter-chunk deadlines.
	streamClient     *http.Client
	streamFirstChunk time.Duration
	streamIdle       time.Duration
	promptCaching    bool
	streamUsage      bool
	sharedClient     *llmclient.Client
	sharedBackend    llmclient.Backend
	prepareRequest   func(*llmprotocol.Request)
}

type ChatRequest struct {
	Model           string                 `json:"model,omitempty"`
	Messages        []protocol.ChatMessage `json:"messages"`
	Tools           []ToolSpec             `json:"tools,omitempty"`
	Stream          bool                   `json:"stream,omitempty"`
	Temperature     float64                `json:"temperature,omitempty"`
	MaxTokens       int                    `json:"max_tokens,omitempty"`
	ReasoningEffort string                 `json:"reasoning_effort,omitempty"`
	// StreamOptions carries OpenAI streaming options; ChatStream sets
	// include_usage so the final chunk reports token usage. Omitted on non-stream
	// requests (the usage block is always present in a non-stream response).
	StreamOptions *StreamOptions `json:"stream_options,omitempty"`
}

// StreamOptions mirrors OpenAI's stream_options. include_usage asks the provider
// to emit a final usage-only chunk on the streamed response.
type StreamOptions struct {
	IncludeUsage bool `json:"include_usage,omitempty"`
}

// Usage is the provider-reported token accounting for one chat completion. Fields
// are best-effort: a provider that omits usage yields a zero Usage.
type Usage struct {
	PromptTokens             int `json:"prompt_tokens,omitempty"`
	CompletionTokens         int `json:"completion_tokens,omitempty"`
	TotalTokens              int `json:"total_tokens,omitempty"`
	CachedInputTokens        int `json:"cached_input_tokens,omitempty"`
	CacheCreationInputTokens int `json:"cache_creation_input_tokens,omitempty"`
}

// UnmarshalJSON accepts both normalized usage fields and the nested OpenAI
// prompt/input-token detail shapes. Providers differ on whether cache creation
// is called creation or write; normalize both names.
func (u *Usage) UnmarshalJSON(data []byte) error {
	type plain Usage
	var wire struct {
		plain
		CacheReadInputTokens  int `json:"cache_read_input_tokens"`
		CacheWriteInputTokens int `json:"cache_write_input_tokens"`
		PromptTokensDetails   struct {
			CachedTokens        int `json:"cached_tokens"`
			CacheCreationTokens int `json:"cache_creation_tokens"`
			CacheWriteTokens    int `json:"cache_write_tokens"`
		} `json:"prompt_tokens_details"`
		InputTokensDetails struct {
			CachedTokens        int `json:"cached_tokens"`
			CacheCreationTokens int `json:"cache_creation_tokens"`
			CacheWriteTokens    int `json:"cache_write_tokens"`
		} `json:"input_tokens_details"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	*u = Usage(wire.plain)
	if u.CachedInputTokens == 0 {
		u.CachedInputTokens = firstNonZero(wire.CacheReadInputTokens, wire.PromptTokensDetails.CachedTokens, wire.InputTokensDetails.CachedTokens)
	}
	if u.CacheCreationInputTokens == 0 {
		u.CacheCreationInputTokens = firstNonZero(
			wire.CacheWriteInputTokens,
			wire.PromptTokensDetails.CacheCreationTokens,
			wire.PromptTokensDetails.CacheWriteTokens,
			wire.InputTokensDetails.CacheCreationTokens,
			wire.InputTokensDetails.CacheWriteTokens,
		)
	}
	return nil
}

func firstNonZero(values ...int) int {
	for _, value := range values {
		if value != 0 {
			return value
		}
	}
	return 0
}

// ToolSpec is a tool exposed to the model. It marshals to the OpenAI
// function-tool shape, which both the sidecar and OpenAI-compatible providers
// understand.
type ToolSpec struct {
	Name        string
	Description string
	Parameters  json.RawMessage // JSON-schema object; empty => {"type":"object"}
}

func (t ToolSpec) MarshalJSON() ([]byte, error) {
	params := t.Parameters
	if len(params) == 0 {
		params = json.RawMessage(`{"type":"object"}`)
	}
	return json.Marshal(map[string]any{
		"type": "function",
		"function": map[string]any{
			"name":        t.Name,
			"description": t.Description,
			"parameters":  params,
		},
	})
}

type ChatResponse struct {
	Message protocol.ChatMessage `json:"message"`
	// Model is the provider-echoed served model ("" if the provider omits it; the
	// caller falls back to the requested model).
	Model string `json:"model,omitempty"`
	// Usage is the provider-reported token accounting (zero when unavailable).
	Usage Usage `json:"usage,omitempty"`
}

func NewOpenAICompatClient(cfg OpenAICompatConfig) (*OpenAICompatClient, error) {
	if cfg.Auth != nil && cfg.AuthHeader != "" {
		return nil, fmt.Errorf("%w: Auth and AuthHeader are mutually exclusive", ErrInvalidProviderConfig)
	}
	parsed, err := url.ParseRequestURI(cfg.BaseURL)
	if err != nil {
		return nil, fmt.Errorf("%w: base url: %w", ErrInvalidProviderConfig, err)
	}
	// An optional Guard enforces config policy (e.g. the sandbox no-real-secret /
	// no-direct-host rule). Core leaves it nil (permissive).
	if cfg.Guard != nil {
		if err := cfg.Guard(parsed, cfg.AuthHeader); err != nil {
			return nil, err
		}
	}
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 60 * time.Second}
	}
	// Streaming gets its own client with NO total timeout — a long generation would
	// otherwise be killed mid-stream ("Client.Timeout ... reading body"). Reuse the
	// base client's transport; ChatStream bounds it by an inter-chunk idle deadline.
	streamClient := &http.Client{Transport: client.Transport, Timeout: 0}
	streamFirstChunk := cfg.StreamFirstChunk
	if streamFirstChunk <= 0 {
		streamFirstChunk = 300 * time.Second // time-to-first-token can be long
	}
	streamIdle := cfg.StreamIdle
	if streamIdle <= 0 {
		streamIdle = 15 * time.Second // once tokens flow, gaps are short
	}
	format := cfg.Format
	if format == "" {
		format = FormatNative
	}
	if format != FormatNative && format != FormatOpenAI && format != FormatResponses {
		return nil, fmt.Errorf("%w: unsupported wire format %q", ErrInvalidProviderConfig, format)
	}
	chatPath := cfg.ChatPath
	if chatPath == "" {
		chatPath = "/v1/chat/completions"
		if format == FormatResponses {
			chatPath = "/v1/responses"
		}
	}
	result := &OpenAICompatClient{baseURL: parsed, chatPath: chatPath, authHeader: cfg.AuthHeader, format: format, client: client, streamClient: streamClient, streamFirstChunk: streamFirstChunk, streamIdle: streamIdle, promptCaching: cfg.PromptCaching, streamUsage: cfg.StreamUsage, prepareRequest: cfg.PrepareRequest}
	if format != FormatNative {
		protocolFormat := llmprotocol.FormatOpenAIChat
		if format == FormatResponses {
			protocolFormat = llmprotocol.FormatOpenAIResponses
		}
		includeUsage := cfg.StreamUsage
		result.sharedClient = llmclient.New(llmclient.Options{
			HTTPClient:         client,
			Policy:             llmprotocol.StrictPolicy(),
			FirstEventTimeout:  streamFirstChunk,
			StreamIdleTimeout:  streamIdle,
			IncludeStreamUsage: &includeUsage,
		})
		sharedAuth := attributedAuthenticator{auth: cfg.Auth, authHeader: cfg.AuthHeader}
		var authenticator llmclient.Authenticator = sharedAuth
		if recoverer, ok := cfg.Auth.(llmclient.UnauthorizedRecoverer); ok {
			authenticator = recoveringAttributedAuthenticator{attributedAuthenticator: sharedAuth, recoverer: recoverer}
		}
		result.sharedBackend = llmclient.Backend{
			Name:    "sahara",
			Format:  protocolFormat,
			BaseURL: cfg.BaseURL,
			Path:    chatPath,
			Auth:    authenticator,
		}
	}
	return result, nil
}

func (c *OpenAICompatClient) BaseURL() string {
	return c.baseURL.String()
}

func (c *OpenAICompatClient) Chat(ctx context.Context, chat ChatRequest) (ChatResponse, error) {
	chat.Messages = sanitizeTranscript(chat.Messages)
	if c.sharedClient != nil {
		request, err := sharedProtocolRequest(chat)
		if err != nil {
			return ChatResponse{}, fmt.Errorf("encode chat request: %w", err)
		}
		c.prepareSharedRequest(&request)
		result, err := c.sharedClient.Call(ctx, c.sharedBackend, request)
		if err != nil {
			return ChatResponse{}, classifySharedClientError(err)
		}
		return sharedProtocolResponse(result.Response)
	}
	body, err := c.encodeRequest(chat)
	if err != nil {
		return ChatResponse{}, fmt.Errorf("marshal chat request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint(), bytes.NewReader(body))
	if err != nil {
		return ChatResponse{}, fmt.Errorf("create chat request: %w", err)
	}
	req.Header.Set("content-type", "application/json")
	if c.authHeader != "" {
		req.Header.Set("authorization", c.authHeader)
	}
	applyAttributionHeaders(ctx, req)
	resp, err := c.client.Do(req)
	if err != nil {
		return ChatResponse{}, classifyTransport(err)
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return ChatResponse{}, fmt.Errorf("read chat response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return ChatResponse{}, httpError(resp.StatusCode, string(data))
	}
	return decodeChatResponse(data)
}

type attributedAuthenticator struct {
	auth       llmclient.Authenticator
	authHeader string
}

func (a attributedAuthenticator) Apply(ctx context.Context, request *http.Request) error {
	if a.auth != nil {
		if err := a.auth.Apply(ctx, request); err != nil {
			return err
		}
	} else if a.authHeader != "" {
		request.Header.Set("Authorization", a.authHeader)
	}
	applyAttributionHeaders(ctx, request)
	return nil
}

type recoveringAttributedAuthenticator struct {
	attributedAuthenticator
	recoverer llmclient.UnauthorizedRecoverer
}

func (a recoveringAttributedAuthenticator) RecoverUnauthorized(ctx context.Context) error {
	return a.recoverer.RecoverUnauthorized(ctx)
}

func (c *OpenAICompatClient) encodeRequest(chat ChatRequest) ([]byte, error) {
	if c.format == FormatOpenAI {
		// The openai wire path has no provider prompt-cache breakpoint concept and
		// drops the meta hint (see lowerMessage); prompt caching is a native-path
		// concern the sidecar lowers to Anthropic cache_control.
		return json.Marshal(toOpenAIRequest(chat))
	}
	if c.promptCaching {
		chat.Messages = stampPromptCacheBreakpoint(chat.Messages)
	}
	return json.Marshal(chat)
}

// stampPromptCacheBreakpoint marks the end of the stable system-prompt prefix
// with a cache breakpoint the sidecar lowers to an Anthropic
// cache_control:{type:"ephemeral"} block. It rides the existing spec Meta
// `scitrera.cache.stable_prefix_chars` convention (no new cross-package fields):
// stable_prefix_chars is a byte boundary supplied by sysprompt. When a valid
// boundary is already present it is authoritative; otherwise a system-only
// message falls back to its complete byte length. The last system message is
// stamped. Returns a copy with that one message cloned+stamped; the caller's
// messages are never mutated in place.
func stampPromptCacheBreakpoint(msgs []protocol.ChatMessage) []protocol.ChatMessage {
	idx := -1
	for i, m := range msgs {
		if m.Role == protocol.RoleSystem {
			idx = i
		}
	}
	if idx < 0 {
		return msgs
	}
	content := messageText(msgs[idx])
	prefix, ok := stablePrefixBytes(msgs[idx], content)
	if !ok {
		prefix = len(content)
	}
	if prefix == 0 {
		return msgs
	}
	sc := map[string]json.RawMessage{}
	if raw, ok := msgs[idx].Meta["scitrera"]; ok && len(raw) > 0 {
		// Preserve any other keys already in the scitrera namespace.
		_ = json.Unmarshal(raw, &sc)
	}
	cache := map[string]json.RawMessage{}
	if raw, ok := sc["cache"]; ok && len(raw) > 0 {
		_ = json.Unmarshal(raw, &cache)
	}
	cache["stable_prefix_chars"] = json.RawMessage(fmt.Sprintf(`%d`, prefix))
	cacheBlob, err := json.Marshal(cache)
	if err != nil {
		return msgs
	}
	sc["cache"] = cacheBlob
	blob, err := json.Marshal(sc)
	if err != nil {
		return msgs
	}
	out := append([]protocol.ChatMessage(nil), msgs...)
	stamped := out[idx].Clone()
	if stamped.Meta == nil {
		stamped.Meta = map[string]json.RawMessage{}
	}
	stamped.Meta["scitrera"] = blob
	out[idx] = stamped
	return out
}

type openAIChatRequest struct {
	Model         string          `json:"model,omitempty"`
	Messages      []openAIMessage `json:"messages"`
	Tools         []ToolSpec      `json:"tools,omitempty"`
	Stream        bool            `json:"stream,omitempty"`
	Temperature   float64         `json:"temperature,omitempty"`
	MaxTokens     int             `json:"max_tokens,omitempty"`
	StreamOptions *StreamOptions  `json:"stream_options,omitempty"`
}

type openAIMessage struct {
	Role       string           `json:"role"`
	Content    any              `json:"content"` // string, or []openAIContentBlock for multimodal
	ToolCalls  []openAIToolCall `json:"tool_calls,omitempty"`
	ToolCallID string           `json:"tool_call_id,omitempty"`
}

// openAIContentBlock is one element of OpenAI's multimodal content array.
type openAIContentBlock struct {
	Type     string          `json:"type"`
	Text     string          `json:"text,omitempty"`
	ImageURL *openAIImageURL `json:"image_url,omitempty"`
}

type openAIImageURL struct {
	URL string `json:"url"`
}

type openAIToolCall struct {
	ID       string             `json:"id"`
	Type     string             `json:"type"`
	Function openAIToolCallFunc `json:"function"`
}

type openAIToolCallFunc struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

// toOpenAIRequest lowers native Scitrera messages to OpenAI chat shape,
// including assistant tool_calls, tool-result messages, and image parts (as
// image_url content blocks) so a tool loop can complete against an
// OpenAI-compatible provider. Images carry only what an external model can
// resolve here — data_uri or a real uri; vfs_ref-only images are dropped on
// this path (the sidecar resolves those in the native path). file/audio parts
// are not lowered.
func toOpenAIRequest(chat ChatRequest) openAIChatRequest {
	msgs := make([]openAIMessage, 0, len(chat.Messages))
	for _, m := range chat.Messages {
		msgs = append(msgs, lowerMessage(m)...)
	}
	return openAIChatRequest{Model: chat.Model, Messages: msgs, Tools: chat.Tools, Stream: chat.Stream, Temperature: chat.Temperature, MaxTokens: chat.MaxTokens, StreamOptions: chat.StreamOptions}
}

// lowerMessage converts one native message into one or more OpenAI messages.
// A message carrying tool_result parts becomes one "tool" message per result;
// otherwise it becomes a single message whose tool_call parts populate
// tool_calls.
func lowerMessage(m protocol.ChatMessage) []openAIMessage {
	var text strings.Builder
	var toolCalls []openAIToolCall
	var toolResults []openAIMessage
	var images []*openAIImageURL
	for _, part := range m.Content {
		switch part.Type() {
		case protocol.ContentText:
			if tp, ok := part.AsText(); ok {
				text.WriteString(tp.Text)
			}
		case protocol.ContentImage:
			if img, ok := part.AsImage(); ok {
				if url := imageURL(img); url != "" {
					images = append(images, &openAIImageURL{URL: url})
				} else {
					// vfs_ref-only (or empty) image: this OpenAI path can't resolve
					// it, so it never reaches the model. Log it rather than dropping
					// silently — this is the line that explains a missing attachment.
					slog.Warn("provider: dropping image part with no model-deliverable carrier on the openai path (vfs_ref-only or empty)",
						slog.String("mime", img.Mime),
						slog.Bool("has_vfs_ref", img.VFSRef != ""),
					)
				}
			}
		case protocol.ContentToolCall:
			if tc, ok := part.AsToolCall(); ok {
				toolCalls = append(toolCalls, openAIToolCall{
					ID:       tc.ID,
					Type:     "function",
					Function: openAIToolCallFunc{Name: tc.Name, Arguments: argsString(tc.Args)},
				})
			}
		case protocol.ContentToolResult:
			if tr, ok := part.AsToolResult(); ok {
				toolResults = append(toolResults, openAIMessage{
					Role:       "tool",
					ToolCallID: tr.CallID,
					Content:    toolResultContent(tr),
				})
			}
		}
	}
	if len(toolResults) > 0 {
		return toolResults
	}
	msg := openAIMessage{Role: openAIRole(m.Role), Content: text.String()}
	if len(images) > 0 {
		// Multimodal: content becomes an array of blocks (text first, then images).
		blocks := make([]openAIContentBlock, 0, len(images)+1)
		if t := text.String(); t != "" {
			blocks = append(blocks, openAIContentBlock{Type: "text", Text: t})
		}
		for _, img := range images {
			blocks = append(blocks, openAIContentBlock{Type: "image_url", ImageURL: img})
		}
		msg.Content = blocks
	}
	if len(toolCalls) > 0 {
		msg.ToolCalls = toolCalls
		if msg.Role != "assistant" {
			msg.Role = "assistant" // tool_calls only valid on assistant
		}
	}
	return []openAIMessage{msg}
}

// imageURL returns the value usable by an external OpenAI-compatible model for
// an image part: an inline data_uri, or a real uri. A vfs_ref-only image yields
// "" (it must be resolved by the sidecar on the native path).
func imageURL(img protocol.ImagePart) string {
	if img.DataURI != "" {
		return img.DataURI
	}
	if img.URI != "" {
		return img.URI
	}
	return ""
}

func argsString(args map[string]json.RawMessage) string {
	if len(args) == 0 {
		return "{}"
	}
	b, err := json.Marshal(args)
	if err != nil {
		return "{}"
	}
	return string(b)
}

func toolResultContent(tr protocol.ToolResultPartBody) string {
	if tr.OutputText != "" {
		return tr.OutputText
	}
	if len(tr.Output) > 0 {
		return string(tr.Output)
	}
	return ""
}

func openAIRole(r protocol.Role) string {
	switch r {
	case protocol.RoleAssistant:
		return "assistant"
	case protocol.RoleSystem:
		return "system"
	case protocol.RoleUser:
		return "user"
	default:
		// tool / tool_result / unknown -> user, the safe default for a plain
		// chat-completions call (avoids tool_call_id requirements).
		return "user"
	}
}

func messageText(m protocol.ChatMessage) string {
	var b strings.Builder
	for _, part := range m.Content {
		if tp, ok := part.AsText(); ok {
			b.WriteString(tp.Text)
		}
	}
	return b.String()
}

func (c *OpenAICompatClient) endpoint() string {
	endpoint := *c.baseURL
	basePath := strings.TrimRight(endpoint.Path, "/")
	chatPath := c.chatPath
	// OpenAI-compatible providers commonly publish their base URL with a
	// trailing /v1, while the harness's default chat path also begins with /v1.
	// Treat those as one version segment instead of producing
	// /v1/v1/chat/completions. Custom base prefixes and custom ChatPath values
	// still compose exactly as configured.
	if strings.HasSuffix(basePath, "/v1") && strings.HasPrefix(chatPath, "/v1/") {
		chatPath = strings.TrimPrefix(chatPath, "/v1")
	}
	endpoint.Path = basePath + chatPath
	return endpoint.String()
}

func decodeChatResponse(data []byte) (ChatResponse, error) {
	var native ChatResponse
	if err := json.Unmarshal(data, &native); err == nil && native.Message.ID != "" {
		return native, nil
	}
	var openAI struct {
		ID      string `json:"id"`
		Model   string `json:"model"`
		Usage   Usage  `json:"usage"`
		Choices []struct {
			Message struct {
				Role      protocol.Role `json:"role"`
				Content   string        `json:"content"`
				ToolCalls []struct {
					ID       string `json:"id"`
					Function struct {
						Name      string          `json:"name"`
						Arguments json.RawMessage `json:"arguments"`
					} `json:"function"`
				} `json:"tool_calls"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(data, &openAI); err != nil {
		return ChatResponse{}, fmt.Errorf("decode chat response: %w", err)
	}
	if len(openAI.Choices) == 0 {
		return ChatResponse{}, fmt.Errorf("decode chat response: no choices")
	}
	msg := openAI.Choices[0].Message
	role := msg.Role
	if role == "" {
		role = protocol.RoleAssistant
	}
	parts := make([]protocol.ContentPart, 0, len(msg.ToolCalls)+1)
	if strings.TrimSpace(msg.Content) != "" {
		part, err := protocol.NewTextPart(msg.Content)
		if err != nil {
			return ChatResponse{}, err
		}
		parts = append(parts, part)
	}
	for i, tc := range msg.ToolCalls {
		id := tc.ID
		if id == "" {
			id = fmt.Sprintf("call-%d", i)
		}
		part, err := protocol.NewToolCallPart(protocol.ToolInvokeEnvelope{
			CallID: id,
			Name:   tc.Function.Name,
			Args:   toolCallArgs(tc.Function.Arguments),
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
	return ChatResponse{
		Message: protocol.ChatMessage{ID: openAI.ID, Role: role, Content: parts},
		Model:   openAI.Model,
		Usage:   openAI.Usage,
	}, nil
}

// toolCallArgs normalizes OpenAI tool-call arguments (a JSON-encoded string, or
// occasionally a raw object) into a spec args map.
func toolCallArgs(raw json.RawMessage) map[string]json.RawMessage {
	if len(raw) == 0 {
		return map[string]json.RawMessage{}
	}
	var asString string
	if err := json.Unmarshal(raw, &asString); err == nil {
		if strings.TrimSpace(asString) == "" {
			return map[string]json.RawMessage{}
		}
		var m map[string]json.RawMessage
		if json.Unmarshal([]byte(asString), &m) == nil {
			return repairToolArgs(m)
		}
		return map[string]json.RawMessage{}
	}
	var m map[string]json.RawMessage
	if json.Unmarshal(raw, &m) == nil {
		return repairToolArgs(m)
	}
	return map[string]json.RawMessage{}
}

// repairToolArgs works around a malformed-tool-argument artifact from some
// upstreams (observed via the MLflow AI Gateway / fireworks for array-valued
// arguments): a structured value is delivered wrapped as {"$text":"<the value
// as a JSON string>"} instead of the value itself, so e.g. every todo item
// arrives as {"$text":"{...}"} and the tool's typed decode sees empty fields.
// Recursively unwrap any such wrapper back to the real object/array. STOPGAP
// until the upstream stops emitting $text; safe because it only fires on a
// single-key {"$text": <string>} whose string parses to an object or array.
func repairToolArgs(m map[string]json.RawMessage) map[string]json.RawMessage {
	for k, raw := range m {
		var v any
		if json.Unmarshal(raw, &v) != nil {
			continue
		}
		if rv, changed := repairDollarText(v); changed {
			if b, err := json.Marshal(rv); err == nil {
				m[k] = b
			}
		}
	}
	return m
}

// repairDollarText returns v with every {"$text":"<json>"} wrapper (whose inner
// string parses to an object or array) replaced by the parsed value, and a bool
// reporting whether anything changed.
func repairDollarText(v any) (any, bool) {
	switch t := v.(type) {
	case map[string]any:
		if len(t) == 1 {
			if s, ok := t["$text"].(string); ok {
				var inner any
				if json.Unmarshal([]byte(s), &inner) == nil {
					switch inner.(type) {
					case map[string]any, []any:
						r, _ := repairDollarText(inner)
						return r, true
					}
				}
			}
		}
		changed := false
		for k, val := range t {
			if nv, c := repairDollarText(val); c {
				t[k] = nv
				changed = true
			}
		}
		return t, changed
	case []any:
		changed := false
		for i, val := range t {
			if nv, c := repairDollarText(val); c {
				t[i] = nv
				changed = true
			}
		}
		return t, changed
	default:
		return v, false
	}
}

// SandboxGuard is the Scitrera-sandbox provider policy (opt-in via
// OpenAICompatConfig.Guard): it rejects a real provider key or a direct provider host
// unless allowDirect is set (sidecar-less testing). The core does not apply it
// by default — the sandbox distribution wires it.
func SandboxGuard(allowDirect bool) Guard {
	return func(base *url.URL, authHeader string) error {
		if allowDirect {
			return nil
		}
		if looksLikeProviderSecret(authHeader) {
			return fmt.Errorf("%w: %w", ErrInvalidProviderConfig, ErrProviderSecret)
		}
		if isDirectProviderHost(base.Hostname()) {
			return fmt.Errorf("%w: %s", ErrDirectProviderURL, base.Hostname())
		}
		return nil
	}
}

func isDirectProviderHost(host string) bool {
	h := strings.ToLower(strings.TrimSpace(host))
	directHosts := []string{
		"api.openai.com",
		"api.anthropic.com",
		"generativelanguage.googleapis.com",
		"api.groq.com",
	}
	for _, direct := range directHosts {
		if h == direct || strings.HasSuffix(h, "."+direct) {
			return true
		}
	}
	return false
}

func looksLikeProviderSecret(value string) bool {
	v := strings.TrimSpace(value)
	if v == "" {
		return false
	}
	lower := strings.ToLower(v)
	if strings.Contains(lower, "placeholder") || strings.Contains(lower, "rewrite") || strings.Contains(lower, "fake") {
		return false
	}
	return strings.HasPrefix(v, "sk-") || strings.HasPrefix(lower, "xai-")
}
