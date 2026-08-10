package main

import (
	"fmt"
	"strings"

	aetherchan "github.com/scitrera/agent-harness-go/pkg/channels/aether"
	"github.com/scitrera/agent-harness-go/pkg/subagent"
	"github.com/scitrera/agent-harness-go/pkg/turn"
)

func aetherSubagentTaskBackend(ch *aetherchan.Channel, cfg appConfig) (subagent.TaskBackend, error) {
	target := strings.TrimSpace(cfg.subagentTarget)
	if target == "" {
		return ch.SubagentTaskBackend(0)
	}
	if !memoryLayerConfigured(cfg.memorylayerMode) {
		return nil, fmt.Errorf("external subagents require MemoryLayer shared history")
	}
	if target == ch.Topic() {
		return nil, fmt.Errorf("--subagent-target resolves to this worker (%s); omit it for in-process execution", target)
	}
	return ch.TargetedSubagentTaskBackend(0, target)
}

func enableAetherSubagentExecutor(ch *aetherchan.Channel, runner *turn.Runner, catalog subagent.Catalog, cfg appConfig) error {
	if !cfg.subagentExecutor {
		return nil
	}
	if !memoryLayerConfigured(cfg.memorylayerMode) {
		return fmt.Errorf("external subagents require MemoryLayer shared history")
	}
	_, err := ch.EnableSubagentExecutor(aetherchan.SubagentExecutorConfig{
		Runner: runner, Catalog: catalog, MaxConcurrency: cfg.subagentExecutorConcurrency,
	})
	return err
}

func externalSubagentLabel(cfg appConfig) string {
	var parts []string
	if cfg.subagentTarget != "" {
		parts = append(parts, "target="+cfg.subagentTarget)
	}
	if cfg.subagentExecutor {
		parts = append(parts, fmt.Sprintf("executor=%d", cfg.subagentExecutorConcurrency))
	}
	if len(parts) == 0 {
		return ""
	}
	return ", subagents: " + strings.Join(parts, " ")
}
