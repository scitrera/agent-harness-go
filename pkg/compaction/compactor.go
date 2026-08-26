package compaction

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

// Compactor turns a full history into a budget-fitting Report. Implementations
// differ in HOW they shed tokens (drop-oldest, evict-parts, summarize); the
// context assembler is agnostic and calls whichever one it is handed.
type Compactor interface {
	Compact(ctx context.Context, messages []protocol.ChatMessage, cfg Config) (Report, error)
}

// EvictionSink offloads a large content blob to durable storage, returning a
// ref (a path/handle) the model can later re-read via read_file. The concrete
// sink is injected so this package stays free of any localtools/store import.
type EvictionSink interface {
	Put(ctx context.Context, key string, content []byte) (ref string, err error)
}

// Summarizer condenses a run of older messages into a single natural-language
// summary. Injected so this package pulls in no provider/LLM dependency.
type Summarizer interface {
	Summarize(ctx context.Context, messages []protocol.ChatMessage) (summary string, err error)
}

// ClassicCompactor is the historical drop-oldest/trim behavior, exposed through
// the Compactor seam. It is a thin wrapper over ReduceWithReport so existing
// callers and tests keep the exact same semantics.
type ClassicCompactor struct{}

func (ClassicCompactor) Compact(_ context.Context, messages []protocol.ChatMessage, cfg Config) (Report, error) {
	return ReduceWithReport(messages, cfg)
}

const (
	defaultEvictKeepRecent  = 6
	defaultEvictTokens      = 20000
	defaultPreviewHeadLines = 5
	defaultPreviewTailLines = 5
)

// EvictingCompactor shrinks large parts in older messages instead of dropping
// whole messages: each oversized tool-result or text part is offloaded to the
// Sink and replaced inline with a head/tail preview plus a read-me-back ref. It
// never touches the last KeepRecent messages, and it never evicts image parts.
// With a nil Sink it is a safe passthrough (delegates to ClassicCompactor).
type EvictingCompactor struct {
	Sink                  EvictionSink
	ToolResultEvictTokens int
	KeepRecent            int
	PreviewHeadLines      int
	PreviewTailLines      int
}

func (c EvictingCompactor) Compact(ctx context.Context, messages []protocol.ChatMessage, cfg Config) (Report, error) {
	if c.Sink == nil {
		return ClassicCompactor{}.Compact(ctx, messages, cfg)
	}
	keepRecent := c.KeepRecent
	if keepRecent <= 0 {
		keepRecent = defaultEvictKeepRecent
	}
	evictTokens := c.ToolResultEvictTokens
	if evictTokens <= 0 {
		evictTokens = defaultEvictTokens
	}
	head := c.PreviewHeadLines
	if head <= 0 {
		head = defaultPreviewHeadLines
	}
	tail := c.PreviewTailLines
	if tail <= 0 {
		tail = defaultPreviewTailLines
	}

	out := cloneMessages(messages)
	// Older = everything before the recent window; only these are candidates.
	evictBefore := len(out) - keepRecent
	for i := 0; i < evictBefore; i++ {
		parts := out[i].Content
		for j, part := range parts {
			content, ok := evictableContent(part, evictTokens)
			if !ok {
				continue
			}
			key := fmt.Sprintf("%s#%d", out[i].ID, j)
			if tr, isTR := part.AsToolResult(); isTR && tr.CallID != "" {
				key = tr.CallID
			}
			ref, err := c.Sink.Put(ctx, key, []byte(content))
			if err != nil {
				return Report{}, fmt.Errorf("evict part: %w", err)
			}
			preview := previewOf(content, head, tail, ref)
			if result, ok := part.AsToolResult(); ok {
				// Keep the tool-result envelope and call linkage intact. Replacing the
				// whole part with plain text leaves a tool-role message that neither
				// Chat Completions nor Responses can represent correctly.
				result.Output = nil
				result.OutputText = preview
				parts[j] = spec.NewToolResultPart(result)
				continue
			}
			replacement, err := protocol.NewTextPart(preview)
			if err != nil {
				return Report{}, fmt.Errorf("evict preview: %w", err)
			}
			parts[j] = replacement
		}
	}

	state := MergeWorldState(ExtractWorldState(messages), cfg.WorldState)
	budget := BudgetFor(out, cfg.MaxTokens, 0)
	out, err := attachReportMetadata(out, state, budget)
	if err != nil {
		return Report{}, err
	}
	return Report{Messages: out, WorldState: state, Budget: budget}, nil
}

// evictableContent returns the readable text of a tool-result or text part when
// its estimated token count exceeds the eviction cap.
// Image (and every other) part type is never evicted.
func evictableContent(part protocol.ContentPart, evictTokens int) (string, bool) {
	if tr, ok := part.AsToolResult(); ok {
		content := string(tr.Output)
		if tr.OutputText != "" {
			content = tr.OutputText
		}
		return content, len(content)/bytesPerToken > evictTokens
	}
	if tp, ok := part.AsText(); ok {
		if len(part.Raw())/bytesPerToken > evictTokens {
			return tp.Text, true
		}
	}
	return "", false
}

// previewOf keeps the first head and last tail lines of content and drops the
// middle behind a marker naming the byte count and the read-me-back ref.
func previewOf(content string, head, tail int, ref string) string {
	lines := strings.Split(content, "\n")
	if head > len(lines) {
		head = len(lines)
	}
	if tail > len(lines)-head {
		tail = len(lines) - head
	}
	var b strings.Builder
	for _, l := range lines[:head] {
		b.WriteString(l)
		b.WriteByte('\n')
	}
	fmt.Fprintf(&b, "[... evicted %d bytes; read the full content at %s ...]", len(content), ref)
	if tail > 0 {
		for _, l := range lines[len(lines)-tail:] {
			b.WriteByte('\n')
			b.WriteString(l)
		}
	}
	return b.String()
}

// SummarizingCompactor folds the older-than-keep-window messages into a single
// schema-versioned system summary when over budget, optionally offloading the
// full dropped run via Sink. OPT-IN only: a nil Summarizer degrades to classic
// behavior, so this is never silently selected as a default.
type SummarizingCompactor struct {
	Summarizer Summarizer
	Sink       EvictionSink
	KeepRecent int
}

func (c SummarizingCompactor) Compact(ctx context.Context, messages []protocol.ChatMessage, cfg Config) (Report, error) {
	if c.Summarizer == nil {
		return ClassicCompactor{}.Compact(ctx, messages, cfg)
	}
	if cfg.MaxTokens <= 0 || EstimateTokens(messages) <= cfg.MaxTokens {
		return ClassicCompactor{}.Compact(ctx, messages, cfg)
	}
	keepRecent := c.KeepRecent
	if keepRecent <= 0 {
		keepRecent = defaultEvictKeepRecent
	}
	keepFrom := len(messages) - keepRecent
	if keepFrom <= 0 {
		return ClassicCompactor{}.Compact(ctx, messages, cfg)
	}
	older := cloneMessages(messages[:keepFrom])
	recent := cloneMessages(messages[keepFrom:])

	summary, err := c.Summarizer.Summarize(ctx, older)
	if err != nil {
		return Report{}, fmt.Errorf("summarize context: %w", err)
	}
	if c.Sink != nil {
		raw, err := json.Marshal(older)
		if err != nil {
			return Report{}, fmt.Errorf("encode summarized run: %w", err)
		}
		key := fmt.Sprintf("summary-offload-%s-%s", older[0].ID, older[len(older)-1].ID)
		if ref, err := c.Sink.Put(ctx, key, raw); err == nil && ref != "" {
			summary = fmt.Sprintf("%s\n[full transcript of %d summarized messages at %s]", summary, len(older), ref)
		}
	}
	head, err := summarySystemMessage(summary, len(older))
	if err != nil {
		return Report{}, err
	}
	out := append([]protocol.ChatMessage{head}, recent...)

	state := MergeWorldState(ExtractWorldState(messages), cfg.WorldState)
	budget := BudgetFor(out, cfg.MaxTokens, len(older))
	out, err = attachReportMetadata(out, state, budget)
	if err != nil {
		return Report{}, err
	}
	return Report{Messages: out, WorldState: state, Budget: budget}, nil
}

func summarySystemMessage(summary string, omitted int) (protocol.ChatMessage, error) {
	part, err := protocol.NewTextPart(summary)
	if err != nil {
		return protocol.ChatMessage{}, err
	}
	return protocol.ChatMessage{
		SchemaVersion: spec.MessagingSchemaVersion,
		ID:            compactionSummaryID,
		Role:          protocol.RoleSystem,
		Content:       []protocol.ContentPart{part},
		Meta:          map[string]json.RawMessage{"omitted_messages": json.RawMessage(strconv.Itoa(omitted))},
	}, nil
}

// CompositeCompactor runs Primary, then falls back to Fallback only when the
// primary's output still overflows cfg.MaxTokens. A nil Primary or Fallback is
// treated as ClassicCompactor.
type CompositeCompactor struct {
	Primary  Compactor
	Fallback Compactor
}

func (c CompositeCompactor) Compact(ctx context.Context, messages []protocol.ChatMessage, cfg Config) (Report, error) {
	primary := c.Primary
	if primary == nil {
		primary = ClassicCompactor{}
	}
	report, err := primary.Compact(ctx, messages, cfg)
	if err != nil {
		return Report{}, err
	}
	if cfg.MaxTokens > 0 && report.Budget.EstimatedTokens > cfg.MaxTokens {
		fallback := c.Fallback
		if fallback == nil {
			fallback = ClassicCompactor{}
		}
		return fallback.Compact(ctx, report.Messages, cfg)
	}
	return report, nil
}

// DefaultCompactor is the standard eviction-first composite: evict oversized
// parts (offloaded to sink), then drop-oldest as a last resort if still over
// budget. A nil sink makes the eviction stage a passthrough.
func DefaultCompactor(sink EvictionSink) Compactor {
	return CompositeCompactor{
		Primary:  EvictingCompactor{Sink: sink},
		Fallback: ClassicCompactor{},
	}
}

// CompactorFor selects a compactor by mode name. "" / "evict" / "composite" ->
// the eviction-first default; "classic" -> plain drop-oldest; "summarize" ->
// summary-then-classic (needs a Summarizer). An unknown mode falls back to the
// default.
func CompactorFor(mode string, sink EvictionSink, summarizer Summarizer) Compactor {
	switch mode {
	case "", "evict", "composite":
		return DefaultCompactor(sink)
	case "classic":
		return ClassicCompactor{}
	case "summarize":
		return CompositeCompactor{
			Primary:  SummarizingCompactor{Summarizer: summarizer, Sink: sink},
			Fallback: ClassicCompactor{},
		}
	default:
		return DefaultCompactor(sink)
	}
}
