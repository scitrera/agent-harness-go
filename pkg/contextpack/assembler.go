package contextpack

import (
	"context"
	"fmt"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/bootstrap"
	"github.com/scitrera/agent-harness-go/pkg/compaction"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/sysprompt"
)

type Config struct {
	MaxHistoryMessages int
	MaxTextPartBytes   int
	// MaxContextTokens, when > 0, caps the estimated tokens of the assembled
	// context (system prompt + history). History is trimmed to fit the budget
	// remaining after the system prompt. 0 disables token budgeting (count/byte
	// caps still apply, plus the turn loop's reactive overflow recovery).
	MaxContextTokens int

	// System-prompt inputs (see internal/sysprompt).
	Base         string
	Tools        []sysprompt.ToolSummary
	Skills       []sysprompt.SkillSummary
	MemoryTools  bool
	MaxFileBytes int
	WorkspaceDir string
	Model        string
	SandboxID    string
	Now          func() time.Time
}

type Assembler struct {
	cfg Config
}

func NewAssembler(cfg Config) Assembler {
	return Assembler{cfg: cfg}
}

func (a Assembler) Build(ctx context.Context, bootstrap []bootstrap.File, history []protocol.ChatMessage) ([]protocol.ChatMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	messages := make([]protocol.ChatMessage, 0, len(history)+1)

	var now time.Time
	if a.cfg.Now != nil {
		now = a.cfg.Now()
	}
	sysMsg, err := sysprompt.Build(sysprompt.Input{
		Base:         a.cfg.Base,
		Bootstrap:    bootstrap,
		Tools:        a.cfg.Tools,
		Skills:       a.cfg.Skills,
		MemoryTools:  a.cfg.MemoryTools,
		MaxFileBytes: a.cfg.MaxFileBytes,
		WorkspaceDir: a.cfg.WorkspaceDir,
		Model:        a.cfg.Model,
		SandboxID:    a.cfg.SandboxID,
		Now:          now,
	}).Message()
	if err != nil {
		return nil, fmt.Errorf("build system prompt: %w", err)
	}
	messages = append(messages, sysMsg)

	historyTokenBudget := 0
	if a.cfg.MaxContextTokens > 0 {
		historyTokenBudget = a.cfg.MaxContextTokens - compaction.EstimateTokens([]protocol.ChatMessage{sysMsg})
		if historyTokenBudget < 1 {
			historyTokenBudget = 1 // keep at least the most recent message
		}
	}
	reduced, err := compaction.Reduce(history, compaction.Config{
		MaxMessages:      a.cfg.MaxHistoryMessages,
		MaxTextPartBytes: a.cfg.MaxTextPartBytes,
		MaxTokens:        historyTokenBudget,
	})
	if err != nil {
		return nil, fmt.Errorf("compact context: %w", err)
	}
	messages = append(messages, reduced...)
	return messages, nil
}
