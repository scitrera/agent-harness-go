// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package requirements

import (
	"fmt"
	"reflect"

	"github.com/scitrera/agent-harness-go/pkg/hooks"
)

type hookEntry struct {
	Hook hooks.CommandHook
}

func (e hookEntry) key() string {
	return string(e.Hook.Event) + "." + e.Hook.Name
}

func (s *mergeState) mergeHooks(layer Layer) error {
	if layer.Hooks == nil {
		return nil
	}
	req := *layer.Hooks
	if hookTimeoutValue(req.Config.DefaultTimeout) > 0 || len(req.Config.EnvAllowlist) > 0 {
		incoming := withSource(req, layer.Source)
		if s.hookConfig != nil && !sameHookConfig(s.hookConfig.Value.Config, req.Config) {
			return conflict("hooks.config", s.hookConfig.Source, layer.Source, "hook runtime config differs")
		}
		s.hookConfig = &incoming
	}
	layerHooks := make([]Sourced[hookEntry], 0, len(req.Hooks))
	for _, hook := range req.Hooks {
		if hook.Name == "" {
			return fmt.Errorf("%w: %s: hook name required", ErrInvalidLayer, layer.Source.display())
		}
		if hook.Event == "" {
			return fmt.Errorf("%w: %s: hook event required", ErrInvalidLayer, layer.Source.display())
		}
		if !validHookEvent(hook.Event) {
			return fmt.Errorf("%w: %s: hook %q has unsupported event %q", ErrInvalidLayer, layer.Source.display(), hook.Name, hook.Event)
		}
		if len(hook.Command) == 0 || hook.Command[0] == "" {
			return fmt.Errorf("%w: %s: hook %q command required", ErrInvalidLayer, layer.Source.display(), hook.Name)
		}
		entry := hookEntry{Hook: hook}
		incoming := withSource(entry, layer.Source)
		existing, ok := s.hookByKey[entry.key()]
		if ok && !equalHook(existing.Value.Hook, hook) {
			return conflict("hooks."+entry.key(), existing.Source, layer.Source, "hook command definitions differ")
		}
		if !ok {
			s.hookByKey[entry.key()] = incoming
			layerHooks = append(layerHooks, incoming)
		}
	}
	s.hooks = append(layerHooks, s.hooks...)
	return nil
}

func validHookEvent(event hooks.EventName) bool {
	switch event {
	case hooks.EventPreToolUse,
		hooks.EventPostToolUse,
		hooks.EventPostToolUseFailure,
		hooks.EventCommand,
		hooks.EventUserPromptSubmit,
		hooks.EventSessionStart,
		hooks.EventSessionEnd,
		hooks.EventTaskCreated,
		hooks.EventTaskCompleted,
		hooks.EventPreCompact,
		hooks.EventPostCompact,
		hooks.EventCWDChanged,
		hooks.EventFileChanged,
		hooks.EventToolLifecycle:
		return true
	default:
		return false
	}
}

func sameHookConfig(a hooks.Config, b hooks.Config) bool {
	return hookTimeoutValue(a.DefaultTimeout) == hookTimeoutValue(b.DefaultTimeout) &&
		reflect.DeepEqual(a.EnvAllowlist, b.EnvAllowlist)
}

func equalHook(a hooks.CommandHook, b hooks.CommandHook) bool {
	return a.Name == b.Name &&
		a.Event == b.Event &&
		hookTimeoutValue(a.Timeout) == hookTimeoutValue(b.Timeout) &&
		reflect.DeepEqual(a.Command, b.Command) &&
		reflect.DeepEqual(a.Env, b.Env)
}

func sourceHookConfig(src *Sourced[HookRequirement]) *Sourced[hooks.Config] {
	if src == nil {
		return nil
	}
	out := withSource(src.Value.Config, src.Source)
	return &out
}

func sourceHooks(entries []Sourced[hookEntry]) []Sourced[hooks.CommandHook] {
	out := make([]Sourced[hooks.CommandHook], 0, len(entries))
	for _, entry := range entries {
		out = append(out, withSource(entry.Value.Hook, entry.Source))
	}
	return out
}
