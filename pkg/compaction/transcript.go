package compaction

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

const (
	RecoverySourceLocal      = "local"
	RecoverySourceTranscript = "transcript"

	RecoveryWarningCorruptSegment = "corrupt_segment"
)

var ErrTranscriptPathRequired = errors.New("compaction: transcript path required")

type LocalTranscriptStore struct {
	path string
}

type TranscriptSegment struct {
	ID         string                 `json:"id"`
	Compacted  bool                   `json:"compacted,omitempty"`
	Messages   []protocol.ChatMessage `json:"messages"`
	WorldState WorldState             `json:"world_state,omitempty"`
	Budget     ContextBudget          `json:"budget,omitempty"`
}

type RecoveryWarning struct {
	Kind    string `json:"kind"`
	Segment int    `json:"segment,omitempty"`
	Detail  string `json:"detail,omitempty"`
}

type RecoveryResult struct {
	Source     string                 `json:"source"`
	Compacted  bool                   `json:"compacted"`
	Messages   []protocol.ChatMessage `json:"messages"`
	WorldState WorldState             `json:"world_state,omitempty"`
	Budget     ContextBudget          `json:"budget,omitempty"`
	Warnings   []RecoveryWarning      `json:"warnings,omitempty"`
}

func NewLocalTranscriptStore(path string) *LocalTranscriptStore {
	return &LocalTranscriptStore{path: path}
}

func (s *LocalTranscriptStore) AppendSegment(ctx context.Context, segment TranscriptSegment) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s == nil || s.path == "" {
		return ErrTranscriptPathRequired
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return fmt.Errorf("create transcript dir: %w", err)
	}
	f, err := os.OpenFile(s.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("open transcript: %w", err)
	}
	defer func() { _ = f.Close() }()
	raw, err := json.Marshal(segment)
	if err != nil {
		return fmt.Errorf("encode transcript segment: %w", err)
	}
	if _, err := f.Write(append(raw, '\n')); err != nil {
		return fmt.Errorf("write transcript segment: %w", err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("sync transcript segment: %w", err)
	}
	return nil
}

func (s *LocalTranscriptStore) Recover(ctx context.Context, localHistory []protocol.ChatMessage) (RecoveryResult, error) {
	if err := ctx.Err(); err != nil {
		return RecoveryResult{}, err
	}
	if s == nil || s.path == "" {
		return RecoveryResult{}, ErrTranscriptPathRequired
	}
	f, err := os.Open(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return localRecovery(localHistory, nil), nil
	}
	if err != nil {
		return RecoveryResult{}, fmt.Errorf("open transcript: %w", err)
	}
	defer func() { _ = f.Close() }()

	var messages []protocol.ChatMessage
	var state WorldState
	var budget ContextBudget
	compacted := false
	decoder := json.NewDecoder(f)
	segmentNumber := 0
	for {
		var segment TranscriptSegment
		if err := decoder.Decode(&segment); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			warnings := []RecoveryWarning{{
				Kind:    RecoveryWarningCorruptSegment,
				Segment: segmentNumber + 1,
				Detail:  err.Error(),
			}}
			return localRecovery(localHistory, warnings), nil
		}
		segmentNumber++
		messages = appendDeduped(messages, segment.Messages...)
		state = MergeWorldState(state, segment.WorldState)
		if segment.Budget.EstimatedTokens > 0 || segment.Budget.MaxTokens > 0 {
			budget = segment.Budget
		}
		compacted = compacted || segment.Compacted
	}
	messages = appendDeduped(messages, localHistory...)
	if len(messages) == 0 {
		return localRecovery(localHistory, nil), nil
	}
	return RecoveryResult{
		Source:     RecoverySourceTranscript,
		Compacted:  compacted,
		Messages:   messages,
		WorldState: state,
		Budget:     budget,
	}, nil
}

func localRecovery(messages []protocol.ChatMessage, warnings []RecoveryWarning) RecoveryResult {
	return RecoveryResult{
		Source:   RecoverySourceLocal,
		Messages: appendDeduped(nil, messages...),
		Warnings: warnings,
	}
}

func appendDeduped(base []protocol.ChatMessage, next ...protocol.ChatMessage) []protocol.ChatMessage {
	seen := make(map[string]struct{}, len(base)+len(next))
	for _, msg := range base {
		if msg.ID != "" {
			seen[msg.ID] = struct{}{}
		}
	}
	out := cloneMessages(base)
	for _, msg := range next {
		if msg.ID != "" {
			if _, ok := seen[msg.ID]; ok {
				continue
			}
			seen[msg.ID] = struct{}{}
		}
		out = append(out, msg.Clone())
	}
	return out
}
