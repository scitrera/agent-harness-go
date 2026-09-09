// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package provider

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"strings"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
	llmclient "github.com/scitrera/go-llm/client"
	llmprotocol "github.com/scitrera/go-llm/protocol"
	openaiprovider "github.com/scitrera/go-llm/provider/openai"
)

func sharedProtocolRequest(chat ChatRequest) (llmprotocol.Request, error) {
	request := llmprotocol.Request{Model: chat.Model, Stream: chat.Stream}
	request.Reasoning.Effort = chat.ReasoningEffort
	if chat.Temperature != 0 {
		value := chat.Temperature
		request.Sampling.Temperature = &value
	}
	if chat.MaxTokens != 0 {
		value := int64(chat.MaxTokens)
		request.Output.MaxTokens = &value
	}
	for _, tool := range chat.Tools {
		parameters := append(json.RawMessage(nil), tool.Parameters...)
		if len(parameters) == 0 {
			parameters = json.RawMessage(`{"type":"object"}`)
		}
		request.Tools = append(request.Tools, llmprotocol.ToolDefinition{
			Type:        "function",
			Name:        tool.Name,
			Description: tool.Description,
			Parameters:  parameters,
		})
	}
	for _, source := range chat.Messages {
		message, err := sharedProtocolMessage(source)
		if err != nil {
			return llmprotocol.Request{}, err
		}
		if source.Role == protocol.RoleSystem {
			request.Instructions = append(request.Instructions, llmprotocol.Instruction{
				Role:    llmprotocol.RoleSystem,
				Content: message.Content,
			})
			continue
		}
		request.Messages = append(request.Messages, message)
	}
	return request, nil
}

func (c *OpenAICompatClient) prepareSharedRequest(request *llmprotocol.Request) {
	if c.format == FormatResponses {
		openaiprovider.PrepareResponsesApplyPatchRequest(request)
	}
	if c.prepareRequest != nil {
		c.prepareRequest(request)
	}
}

func sharedProtocolMessage(source protocol.ChatMessage) (llmprotocol.Message, error) {
	message := llmprotocol.Message{ID: source.ID, Role: sharedRole(source.Role)}
	for _, part := range source.Content {
		switch part.Type() {
		case protocol.ContentText:
			if value, ok := part.AsText(); ok {
				message.Content = append(message.Content, llmprotocol.Text(value.Text))
			}
		case protocol.ContentReasoning:
			var value protocol.ReasoningPart
			if err := part.Decode(&value); err == nil {
				message.Content = append(message.Content, llmprotocol.ContentBlock{
					Type: llmprotocol.ContentReasoning,
					Text: value.Text,
				})
			}
		case protocol.ContentImage:
			if value, ok := part.AsImage(); ok {
				url := imageURL(value)
				if url == "" {
					slog.Warn("provider: dropping image part with no model-deliverable carrier",
						slog.String("mime", value.Mime), slog.Bool("has_vfs_ref", value.VFSRef != ""))
					continue
				}
				message.Content = append(message.Content, llmprotocol.ContentBlock{
					Type:   llmprotocol.ContentImage,
					Source: &llmprotocol.Source{Kind: "url", URL: url, MediaType: value.Mime},
				})
			}
		case protocol.ContentFile:
			if value, ok := part.AsFile(); ok {
				slog.Warn("provider: dropping unresolved file part on HTTP LLM path",
					slog.String("mime", value.Mime), slog.Bool("has_vfs_ref", value.VFSRef != ""))
			}
		case protocol.ContentToolCall:
			if value, ok := part.AsToolCall(); ok {
				arguments, err := json.Marshal(value.Args)
				if err != nil {
					return llmprotocol.Message{}, fmt.Errorf("encode tool call arguments: %w", err)
				}
				message.Content = append(message.Content, llmprotocol.ContentBlock{
					Type: llmprotocol.ContentToolCall,
					ToolCall: &llmprotocol.ToolCall{
						ID: value.ID, Name: value.Name, Arguments: arguments,
					},
				})
			}
		case protocol.ContentToolResult:
			if value, ok := part.AsToolResult(); ok {
				text := toolResultContent(value)
				isError := value.IsError
				message.Role = llmprotocol.RoleTool
				message.Content = append(message.Content, llmprotocol.ContentBlock{
					Type: llmprotocol.ContentToolResult,
					Result: &llmprotocol.ToolResult{
						ToolCallID: value.CallID,
						Content:    []llmprotocol.ContentBlock{llmprotocol.Text(text)},
						IsError:    &isError,
					},
				})
			}
		}
	}
	return message, nil
}

func sharedRole(role protocol.Role) llmprotocol.Role {
	switch role {
	case protocol.RoleSystem:
		return llmprotocol.RoleSystem
	case protocol.RoleAssistant:
		return llmprotocol.RoleAssistant
	case protocol.RoleTool, protocol.RoleToolResult:
		return llmprotocol.RoleTool
	default:
		return llmprotocol.RoleUser
	}
}

func sharedProtocolResponse(response llmprotocol.Response) (ChatResponse, error) {
	message := protocol.ChatMessage{ID: response.ID, Role: protocol.RoleAssistant}
	for _, output := range response.Outputs {
		if output.Role != "" {
			message.Role = protocol.Role(output.Role)
		}
		for _, block := range output.Content {
			switch block.Type {
			case llmprotocol.ContentText, llmprotocol.ContentRefusal:
				part, err := protocol.NewTextPart(block.Text)
				if err != nil {
					return ChatResponse{}, err
				}
				message.Content = append(message.Content, part)
			case llmprotocol.ContentReasoning:
				part, err := protocol.NewReasoningPart(block.Text, block.Signature != "")
				if err != nil {
					return ChatResponse{}, err
				}
				message.Content = append(message.Content, part)
			case llmprotocol.ContentToolCall:
				if block.ToolCall == nil {
					continue
				}
				part, err := protocol.NewToolCallPart(protocol.ToolInvokeEnvelope{
					CallID: block.ToolCall.ID,
					Name:   block.ToolCall.Name,
					Args:   protocol.RawToArgs(block.ToolCall.Arguments),
				})
				if err != nil {
					return ChatResponse{}, err
				}
				message.Content = append(message.Content, part)
			}
		}
	}
	if len(message.Content) == 0 {
		part, err := protocol.NewTextPart("")
		if err != nil {
			return ChatResponse{}, err
		}
		message.Content = append(message.Content, part)
	}
	return ChatResponse{
		Message: message,
		Model:   response.Model,
		Usage: Usage{
			PromptTokens:             sharedTokenCount(response.Usage.InputTokens),
			CompletionTokens:         sharedTokenCount(response.Usage.OutputTokens),
			TotalTokens:              sharedTokenCount(response.Usage.TotalTokens),
			CachedInputTokens:        sharedTokenCount(response.Usage.CachedInputTokens),
			CacheCreationInputTokens: sharedTokenCount(response.Usage.CacheCreationTokens),
		},
	}, nil
}

func sharedTokenCount(value *int64) int {
	if value == nil || *value <= 0 {
		return 0
	}
	if *value > int64(math.MaxInt) {
		return math.MaxInt
	}
	return int(*value)
}

func classifySharedClientError(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	var clientError *llmclient.Error
	if !errors.As(err, &clientError) {
		return classifyTransport(err)
	}
	if clientError.StatusCode != 0 {
		body := strings.TrimSpace(string(clientError.Body))
		if body == "" {
			body = clientError.Message
		}
		return httpError(clientError.StatusCode, body)
	}
	switch clientError.Kind {
	case llmclient.ErrorCanceled:
		return context.Canceled
	case llmclient.ErrorAuthentication:
		return &ProviderError{Kind: FailureAuth, wrapped: err}
	case llmclient.ErrorTimeout:
		return &ProviderError{Kind: FailureTimeout, wrapped: err}
	case llmclient.ErrorTransport, llmclient.ErrorStream:
		return &ProviderError{Kind: FailureNetwork, wrapped: err}
	case llmclient.ErrorEncode, llmclient.ErrorInvalidConfig:
		return &ProviderError{Kind: FailureBadRequest, wrapped: err}
	default:
		return &ProviderError{Kind: FailureServer, wrapped: err}
	}
}

func (c *OpenAICompatClient) sharedChatStream(
	ctx context.Context,
	chat ChatRequest,
	onDelta DeltaFunc,
) (ChatResponse, error) {
	request, err := sharedProtocolRequest(chat)
	if err != nil {
		return ChatResponse{}, fmt.Errorf("encode chat request: %w", err)
	}
	c.prepareSharedRequest(&request)
	stream, err := c.sharedClient.Stream(ctx, c.sharedBackend, request)
	if err != nil {
		return ChatResponse{}, classifySharedClientError(err)
	}
	defer func() { _ = stream.Close() }()
	for {
		event, nextErr := stream.Next(ctx)
		if errors.Is(nextErr, io.EOF) {
			break
		}
		if nextErr != nil {
			return ChatResponse{}, classifySharedClientError(nextErr)
		}
		switch event.Type {
		case llmprotocol.StreamTextDelta, llmprotocol.StreamRefusalDelta:
			if event.Delta != "" {
				if err := onDelta(DeltaText, event.Delta); err != nil {
					return ChatResponse{}, err
				}
			}
		case llmprotocol.StreamReasoningDelta:
			// Forwarded on its own channel so the consumer can stream the
			// thinking trace live into its own content part. The signature
			// deltas (StreamReasoningSignatureDelta) are NOT forwarded: they
			// carry no user-visible text and only set the assembled block's
			// redaction marker, which arrives with stream.Response() below.
			if event.Delta != "" {
				if err := onDelta(DeltaReasoning, event.Delta); err != nil {
					return ChatResponse{}, err
				}
			}
		case llmprotocol.StreamError:
			message := "upstream LLM stream failed"
			if event.Error != nil && event.Error.Message != "" {
				message = event.Error.Message
			}
			return ChatResponse{}, &ProviderError{
				Kind:    FailureServer,
				Body:    message,
				wrapped: fmt.Errorf("%w: %s", ErrProviderHTTP, message),
			}
		}
	}
	return sharedProtocolResponse(stream.Response())
}
