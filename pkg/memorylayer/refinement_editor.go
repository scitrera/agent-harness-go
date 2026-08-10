package memorylayer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	memorylayersdk "github.com/scitrera/memorylayer/memorylayer-sdk-go"

	"github.com/scitrera/agent-harness-go/pkg/refinement"
)

const maxRefinementHistoryPages = 10

// RefinementResourceEditor applies conflict-checked prompt-note, memory,
// agent-specification, and skill-manifest edits against MemoryLayer's
// revisioned native resource APIs.
type RefinementResourceEditor struct {
	client *memorylayersdk.Client
}

type refinementResourceEditorConfig struct {
	transport memorylayersdk.Transport
}

type RefinementResourceEditorOption func(*refinementResourceEditorConfig)

func WithRefinementResourceTransport(transport memorylayersdk.Transport) RefinementResourceEditorOption {
	return func(config *refinementResourceEditorConfig) { config.transport = transport }
}

func NewRefinementResourceEditor(cfg Config, editorOptions ...RefinementResourceEditorOption) (*RefinementResourceEditor, error) {
	editorConfig := refinementResourceEditorConfig{}
	for _, option := range editorOptions {
		if option != nil {
			option(&editorConfig)
		}
	}
	opts := []memorylayersdk.Option{
		memorylayersdk.WithAPIKey(cfg.APIKey),
		memorylayersdk.WithWorkspaceID(cfg.Workspace),
	}
	if editorConfig.transport != nil {
		opts = append(opts, memorylayersdk.WithTransport(editorConfig.transport))
	} else {
		if cfg.BaseURL == "" {
			return nil, fmt.Errorf("memorylayer: refinement resource base url or transport required")
		}
		opts = append(opts, memorylayersdk.WithBaseURL(cfg.BaseURL))
	}
	if cfg.HTTPClient != nil && editorConfig.transport == nil {
		opts = append(opts, memorylayersdk.WithHTTPClient(cfg.HTTPClient))
	}
	client, err := memorylayersdk.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("memorylayer: refinement resource client: %w", err)
	}
	return &RefinementResourceEditor{client: client}, nil
}

func (e *RefinementResourceEditor) Apply(ctx context.Context, workspaceID, operationID string, edit refinement.Edit) (refinement.Mutation, error) {
	if e == nil || e.client == nil {
		return refinement.Mutation{}, fmt.Errorf("%w: memorylayer editor is not configured", refinement.ErrInvalid)
	}
	if strings.TrimSpace(operationID) == "" {
		return refinement.Mutation{}, fmt.Errorf("%w: operation id is required", refinement.ErrInvalid)
	}
	switch edit.ResourceKind {
	case refinement.ResourcePromptNote:
		return e.applyPromptNote(ctx, workspaceID, operationID, edit)
	case refinement.ResourceMemory:
		return e.applyMemory(ctx, workspaceID, operationID, edit)
	case refinement.ResourceAgentSpecification:
		return e.applyAgentSpecification(ctx, workspaceID, operationID, edit)
	case refinement.ResourceSkill:
		return e.applySkill(ctx, workspaceID, operationID, edit)
	default:
		return refinement.Mutation{}, fmt.Errorf("%w: %s", refinement.ErrUnsupportedResource, edit.ResourceKind)
	}
}

func (e *RefinementResourceEditor) applyMemory(ctx context.Context, workspaceID, operationID string, edit refinement.Edit) (refinement.Mutation, error) {
	authority := promptNoteAuthority(ctx)
	opts := memorylayersdk.MemoryMutationOptions{
		WorkspaceID: workspaceID, IdempotencyKey: operationID, ETag: edit.ExpectedETag, Authority: authority,
	}
	switch edit.Action {
	case refinement.ActionCreate:
		var input memorylayersdk.SemanticMemoryCreateInput
		if err := decodeRefinementContent(edit.Content, &input); err != nil {
			return refinement.Mutation{}, err
		}
		input.LogicalKey = edit.ResourceKey
		result, err := e.client.CreateMemoryVersioned(ctx, input, opts)
		if err != nil {
			return refinement.Mutation{}, mapRefinementMutationError("create memory", err)
		}
		return refinement.Mutation{After: memorySnapshot(result.Memory), Replayed: result.Replayed}, nil
	case refinement.ActionReplace, refinement.ActionDelete, refinement.ActionRestore:
		if strings.TrimSpace(edit.ResourceID) == "" {
			return refinement.Mutation{}, fmt.Errorf("%w: memory %s requires resource_id", refinement.ErrInvalid, edit.Action)
		}
		current, err := e.client.GetMemory(ctx, edit.ResourceID, memorylayersdk.GetMemoryOptions{
			WorkspaceID: workspaceID, IncludeDeleted: true, Authority: authority,
		})
		if err != nil {
			return refinement.Mutation{}, mapRefinementMutationError("get memory", err)
		}
		if current.LogicalKey == nil || *current.LogicalKey != edit.ResourceKey {
			return refinement.Mutation{}, fmt.Errorf("%w: memory id %s has logical key %q, not %q", refinement.ErrConflict, edit.ResourceID, optionalStringValue(current.LogicalKey), edit.ResourceKey)
		}
		var result *memorylayersdk.MemoryMutationResult
		switch edit.Action {
		case refinement.ActionReplace:
			var input memorylayersdk.SemanticMemoryReplaceInput
			if err := decodeRefinementContent(edit.Content, &input); err != nil {
				return refinement.Mutation{}, err
			}
			result, err = e.client.ReplaceSemanticMemory(ctx, edit.ResourceID, input, opts)
		case refinement.ActionDelete:
			result, err = e.client.DeleteMemoryVersioned(ctx, edit.ResourceID, opts)
		case refinement.ActionRestore:
			result, err = e.client.RestoreMemoryVersioned(ctx, edit.ResourceID, opts)
		}
		if err != nil {
			return refinement.Mutation{}, mapRefinementMutationError(string(edit.Action)+" memory", err)
		}
		before := memorySnapshot(*current)
		expectedDeleted := edit.Action == refinement.ActionRestore
		if before.ETag != edit.ExpectedETag || before.Deleted != expectedDeleted {
			if !result.Replayed {
				return refinement.Mutation{}, fmt.Errorf("%w: authority accepted a memory mutation whose observed head did not match its expected ETag", refinement.ErrConflict)
			}
			before, err = e.findMemorySnapshot(ctx, workspaceID, edit.ResourceID, edit.ExpectedETag, authority)
			if err != nil {
				return refinement.Mutation{}, err
			}
		}
		return refinement.Mutation{Before: before, After: memorySnapshot(result.Memory), Replayed: result.Replayed}, nil
	default:
		return refinement.Mutation{}, fmt.Errorf("%w: unsupported memory action %q", refinement.ErrInvalid, edit.Action)
	}
}

func (e *RefinementResourceEditor) applySkill(ctx context.Context, workspaceID, operationID string, edit refinement.Edit) (refinement.Mutation, error) {
	authority := promptNoteAuthority(ctx)
	opts := memorylayersdk.SkillManifestMutationOptions{
		WorkspaceID: workspaceID, IdempotencyKey: operationID, ETag: edit.ExpectedETag, Authority: authority,
	}
	switch edit.Action {
	case refinement.ActionCreate:
		var input memorylayersdk.SkillManifestCreateInput
		if err := decodeRefinementContent(edit.Content, &input); err != nil {
			return refinement.Mutation{}, err
		}
		input.Name = edit.ResourceKey
		result, err := e.client.Skills.CreateVersioned(ctx, input, opts)
		if err != nil {
			return refinement.Mutation{}, mapRefinementMutationError("create skill manifest", err)
		}
		return refinement.Mutation{After: skillSnapshot(result.Skill), Replayed: result.Replayed}, nil
	case refinement.ActionReplace, refinement.ActionDelete, refinement.ActionRestore:
		if strings.TrimSpace(edit.ResourceID) == "" {
			return refinement.Mutation{}, fmt.Errorf("%w: skill %s requires resource_id", refinement.ErrInvalid, edit.Action)
		}
		current, err := e.client.Skills.GetWithOptions(ctx, edit.ResourceID, memorylayersdk.SkillGetOptions{
			WorkspaceID: workspaceID, IncludeDeleted: true, Authority: authority,
		})
		if err != nil {
			return refinement.Mutation{}, mapRefinementMutationError("get skill manifest", err)
		}
		if current.Name != edit.ResourceKey {
			return refinement.Mutation{}, fmt.Errorf("%w: skill id %s has name %q, not %q", refinement.ErrConflict, edit.ResourceID, current.Name, edit.ResourceKey)
		}
		var result *memorylayersdk.SkillMutationResult
		switch edit.Action {
		case refinement.ActionReplace:
			var input memorylayersdk.SkillManifestReplaceInput
			if err := decodeRefinementContent(edit.Content, &input); err != nil {
				return refinement.Mutation{}, err
			}
			result, err = e.client.Skills.ReplaceManifest(ctx, edit.ResourceID, input, opts)
		case refinement.ActionDelete:
			result, err = e.client.Skills.DeleteVersioned(ctx, edit.ResourceID, opts)
		case refinement.ActionRestore:
			result, err = e.client.Skills.Restore(ctx, edit.ResourceID, opts)
		}
		if err != nil {
			return refinement.Mutation{}, mapRefinementMutationError(string(edit.Action)+" skill manifest", err)
		}
		before := skillSnapshot(*current)
		expectedDeleted := edit.Action == refinement.ActionRestore
		if before.ETag != edit.ExpectedETag || before.Deleted != expectedDeleted {
			if !result.Replayed {
				return refinement.Mutation{}, fmt.Errorf("%w: authority accepted a skill mutation whose observed head did not match its expected ETag", refinement.ErrConflict)
			}
			before, err = e.findSkillSnapshot(ctx, workspaceID, edit.ResourceID, edit.ExpectedETag, authority)
			if err != nil {
				return refinement.Mutation{}, err
			}
		}
		return refinement.Mutation{Before: before, After: skillSnapshot(result.Skill), Replayed: result.Replayed}, nil
	default:
		return refinement.Mutation{}, fmt.Errorf("%w: unsupported skill action %q", refinement.ErrInvalid, edit.Action)
	}
}

func (e *RefinementResourceEditor) applyPromptNote(ctx context.Context, workspaceID, operationID string, edit refinement.Edit) (refinement.Mutation, error) {
	authority := promptNoteAuthority(ctx)
	opts := memorylayersdk.PromptNoteMutationOptions{
		WorkspaceID: workspaceID, IdempotencyKey: operationID, ETag: edit.ExpectedETag, Authority: authority,
	}
	switch edit.Action {
	case refinement.ActionCreate:
		var input memorylayersdk.PromptNoteCreateInput
		if err := decodeRefinementContent(edit.Content, &input); err != nil {
			return refinement.Mutation{}, err
		}
		input.Key = edit.ResourceKey
		result, err := e.client.PromptNotes.Create(ctx, input, opts)
		if err != nil {
			return refinement.Mutation{}, mapRefinementMutationError("create prompt note", err)
		}
		return refinement.Mutation{After: promptNoteSnapshot(result.Note), Replayed: result.Replayed}, nil
	case refinement.ActionReplace, refinement.ActionDelete, refinement.ActionRestore:
		if strings.TrimSpace(edit.ResourceID) == "" {
			return refinement.Mutation{}, fmt.Errorf("%w: prompt-note %s requires resource_id", refinement.ErrInvalid, edit.Action)
		}
		current, err := e.client.PromptNotes.Get(ctx, edit.ResourceID, memorylayersdk.PromptNoteGetOptions{
			WorkspaceID: workspaceID, IncludeDeleted: true, Authority: authority,
		})
		if err != nil {
			return refinement.Mutation{}, mapRefinementMutationError("get prompt note", err)
		}
		if current.Key != edit.ResourceKey {
			return refinement.Mutation{}, fmt.Errorf("%w: prompt-note id %s has key %q, not %q", refinement.ErrConflict, edit.ResourceID, current.Key, edit.ResourceKey)
		}
		var result *memorylayersdk.PromptNoteMutationResult
		switch edit.Action {
		case refinement.ActionReplace:
			var input memorylayersdk.PromptNoteReplaceInput
			if err := decodeRefinementContent(edit.Content, &input); err != nil {
				return refinement.Mutation{}, err
			}
			result, err = e.client.PromptNotes.Replace(ctx, edit.ResourceID, input, opts)
		case refinement.ActionDelete:
			result, err = e.client.PromptNotes.Delete(ctx, edit.ResourceID, opts)
		case refinement.ActionRestore:
			result, err = e.client.PromptNotes.Restore(ctx, edit.ResourceID, opts)
		}
		if err != nil {
			return refinement.Mutation{}, mapRefinementMutationError(string(edit.Action)+" prompt note", err)
		}
		before := promptNoteSnapshot(*current)
		expectedDeleted := edit.Action == refinement.ActionRestore
		if before.ETag != edit.ExpectedETag || before.Deleted != expectedDeleted {
			if !result.Replayed {
				return refinement.Mutation{}, fmt.Errorf("%w: authority accepted a mutation whose observed head did not match its expected ETag", refinement.ErrConflict)
			}
			before, err = e.findPromptNoteSnapshot(ctx, workspaceID, edit.ResourceID, edit.ExpectedETag, authority)
			if err != nil {
				return refinement.Mutation{}, err
			}
		}
		return refinement.Mutation{Before: before, After: promptNoteSnapshot(result.Note), Replayed: result.Replayed}, nil
	default:
		return refinement.Mutation{}, fmt.Errorf("%w: unsupported prompt-note action %q", refinement.ErrInvalid, edit.Action)
	}
}

func (e *RefinementResourceEditor) applyAgentSpecification(ctx context.Context, workspaceID, operationID string, edit refinement.Edit) (refinement.Mutation, error) {
	authority := promptNoteAuthority(ctx)
	opts := memorylayersdk.AgentSpecificationMutationOptions{
		WorkspaceID: workspaceID, IdempotencyKey: operationID, ETag: edit.ExpectedETag, Authority: authority,
	}
	switch edit.Action {
	case refinement.ActionCreate:
		var input memorylayersdk.AgentSpecificationCreateInput
		if err := decodeRefinementContent(edit.Content, &input); err != nil {
			return refinement.Mutation{}, err
		}
		input.Key = edit.ResourceKey
		result, err := e.client.AgentSpecifications.Create(ctx, input, opts)
		if err != nil {
			return refinement.Mutation{}, mapRefinementMutationError("create agent specification", err)
		}
		return refinement.Mutation{After: agentSpecificationSnapshot(result.Specification), Replayed: result.Replayed}, nil
	case refinement.ActionReplace, refinement.ActionDelete, refinement.ActionRestore:
		if strings.TrimSpace(edit.ResourceID) == "" {
			return refinement.Mutation{}, fmt.Errorf("%w: agent-specification %s requires resource_id", refinement.ErrInvalid, edit.Action)
		}
		current, err := e.client.AgentSpecifications.Get(ctx, edit.ResourceID, memorylayersdk.AgentSpecificationGetOptions{
			WorkspaceID: workspaceID, IncludeDeleted: true, Authority: authority,
		})
		if err != nil {
			return refinement.Mutation{}, mapRefinementMutationError("get agent specification", err)
		}
		if current.Key != edit.ResourceKey {
			return refinement.Mutation{}, fmt.Errorf("%w: agent-specification id %s has key %q, not %q", refinement.ErrConflict, edit.ResourceID, current.Key, edit.ResourceKey)
		}
		var result *memorylayersdk.AgentSpecificationMutationResult
		switch edit.Action {
		case refinement.ActionReplace:
			var input memorylayersdk.AgentSpecificationReplaceInput
			if err := decodeRefinementContent(edit.Content, &input); err != nil {
				return refinement.Mutation{}, err
			}
			result, err = e.client.AgentSpecifications.Replace(ctx, edit.ResourceID, input, opts)
		case refinement.ActionDelete:
			result, err = e.client.AgentSpecifications.Delete(ctx, edit.ResourceID, opts)
		case refinement.ActionRestore:
			result, err = e.client.AgentSpecifications.Restore(ctx, edit.ResourceID, opts)
		}
		if err != nil {
			return refinement.Mutation{}, mapRefinementMutationError(string(edit.Action)+" agent specification", err)
		}
		before := agentSpecificationSnapshot(*current)
		expectedDeleted := edit.Action == refinement.ActionRestore
		if before.ETag != edit.ExpectedETag || before.Deleted != expectedDeleted {
			if !result.Replayed {
				return refinement.Mutation{}, fmt.Errorf("%w: authority accepted a mutation whose observed head did not match its expected ETag", refinement.ErrConflict)
			}
			before, err = e.findAgentSpecificationSnapshot(ctx, workspaceID, edit.ResourceID, edit.ExpectedETag, authority)
			if err != nil {
				return refinement.Mutation{}, err
			}
		}
		return refinement.Mutation{Before: before, After: agentSpecificationSnapshot(result.Specification), Replayed: result.Replayed}, nil
	default:
		return refinement.Mutation{}, fmt.Errorf("%w: unsupported agent-specification action %q", refinement.ErrInvalid, edit.Action)
	}
}

func (e *RefinementResourceEditor) findPromptNoteSnapshot(ctx context.Context, workspaceID, resourceID, etag string, authority *memorylayersdk.AuthorityContext) (*refinement.ResourceSnapshot, error) {
	pageToken := ""
	for page := 0; page < maxRefinementHistoryPages; page++ {
		history, err := e.client.PromptNotes.History(ctx, resourceID, memorylayersdk.PromptNoteHistoryOptions{
			WorkspaceID: workspaceID, Limit: 100, PageToken: pageToken, Authority: authority,
		})
		if err != nil {
			return nil, mapRefinementMutationError("read prompt-note history", err)
		}
		for _, revision := range history.Revisions {
			if revision.Note.ETag == etag {
				return promptNoteSnapshot(revision.Note), nil
			}
		}
		if history.NextPageToken == "" {
			break
		}
		pageToken = history.NextPageToken
	}
	return nil, fmt.Errorf("%w: expected prompt-note revision %s is absent from bounded history", refinement.ErrConflict, etag)
}

func (e *RefinementResourceEditor) findAgentSpecificationSnapshot(ctx context.Context, workspaceID, resourceID, etag string, authority *memorylayersdk.AuthorityContext) (*refinement.ResourceSnapshot, error) {
	pageToken := ""
	for page := 0; page < maxRefinementHistoryPages; page++ {
		history, err := e.client.AgentSpecifications.History(ctx, resourceID, memorylayersdk.AgentSpecificationHistoryOptions{
			WorkspaceID: workspaceID, Limit: 100, PageToken: pageToken, Authority: authority,
		})
		if err != nil {
			return nil, mapRefinementMutationError("read agent-specification history", err)
		}
		for _, revision := range history.Revisions {
			if revision.Specification.ETag == etag {
				return agentSpecificationSnapshot(revision.Specification), nil
			}
		}
		if history.NextPageToken == "" {
			break
		}
		pageToken = history.NextPageToken
	}
	return nil, fmt.Errorf("%w: expected agent-specification revision %s is absent from bounded history", refinement.ErrConflict, etag)
}

func (e *RefinementResourceEditor) findSkillSnapshot(ctx context.Context, workspaceID, resourceID, etag string, authority *memorylayersdk.AuthorityContext) (*refinement.ResourceSnapshot, error) {
	pageToken := ""
	for page := 0; page < maxRefinementHistoryPages; page++ {
		history, err := e.client.Skills.History(ctx, resourceID, memorylayersdk.SkillHistoryOptions{
			WorkspaceID: workspaceID, Limit: 100, PageToken: pageToken, Authority: authority,
		})
		if err != nil {
			return nil, mapRefinementMutationError("read skill history", err)
		}
		for _, revision := range history.Revisions {
			if revision.Skill.ETag == etag {
				return skillSnapshot(revision.Skill), nil
			}
		}
		if history.NextPageToken == "" {
			break
		}
		pageToken = history.NextPageToken
	}
	return nil, fmt.Errorf("%w: expected skill revision %s is absent from bounded history", refinement.ErrConflict, etag)
}

func (e *RefinementResourceEditor) findMemorySnapshot(ctx context.Context, workspaceID, resourceID, etag string, authority *memorylayersdk.AuthorityContext) (*refinement.ResourceSnapshot, error) {
	pageToken := ""
	for page := 0; page < maxRefinementHistoryPages; page++ {
		history, err := e.client.MemoryHistory(ctx, resourceID, memorylayersdk.MemoryHistoryOptions{
			WorkspaceID: workspaceID, Limit: 100, PageToken: pageToken, Authority: authority,
		})
		if err != nil {
			return nil, mapRefinementMutationError("read memory history", err)
		}
		for _, revision := range history.Revisions {
			if revision.Memory.ETag == etag {
				return memorySnapshot(revision.Memory), nil
			}
		}
		if history.NextPageToken == "" {
			break
		}
		pageToken = history.NextPageToken
	}
	return nil, fmt.Errorf("%w: expected memory revision %s is absent from bounded history", refinement.ErrConflict, etag)
}

func decodeRefinementContent(content map[string]any, output any) error {
	encoded, err := json.Marshal(content)
	if err != nil {
		return fmt.Errorf("%w: encode refinement content: %v", refinement.ErrInvalid, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(output); err != nil {
		return fmt.Errorf("%w: decode refinement content: %v", refinement.ErrInvalid, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: refinement content contains trailing JSON", refinement.ErrInvalid)
	}
	return nil
}

func promptNoteSnapshot(note memorylayersdk.PromptNote) *refinement.ResourceSnapshot {
	return &refinement.ResourceSnapshot{
		ResourceID: note.ID, ResourceKey: note.Key, ETag: note.ETag, SchemaVersion: note.SchemaVersion,
		Content: map[string]any{
			"title": note.Title, "content": note.Content, "enabled": note.Enabled,
			"schema_version": note.SchemaVersion, "metadata": note.Metadata,
		},
		Metadata: note.Metadata, Deleted: note.DeletedAt != nil,
	}
}

func agentSpecificationSnapshot(specification memorylayersdk.AgentSpecification) *refinement.ResourceSnapshot {
	return &refinement.ResourceSnapshot{
		ResourceID: specification.ID, ResourceKey: specification.Key, ETag: specification.ETag, SchemaVersion: specification.SchemaVersion,
		Content: map[string]any{
			"name": specification.Name, "description": specification.Description, "instructions": specification.Instructions,
			"invocation_guidance": specification.InvocationGuidance, "model": specification.Model, "max_turns": specification.MaxTurns,
			"allowed_tools": specification.AllowedTools, "denied_tools": specification.DeniedTools, "skills": specification.Skills,
			"mcp_servers": specification.MCPServers, "permission_mode": specification.PermissionMode,
			"exec_policy_hint": specification.ExecPolicyHint, "background": specification.Background, "enabled": specification.Enabled,
			"schema_version": specification.SchemaVersion, "metadata": specification.Metadata,
		},
		Metadata: specification.Metadata, Deleted: specification.DeletedAt != nil,
	}
}

func skillSnapshot(skill memorylayersdk.SkillModel) *refinement.ResourceSnapshot {
	return &refinement.ResourceSnapshot{
		ResourceID: skill.ID, ResourceKey: skill.Name, ETag: skill.ETag, SchemaVersion: 1,
		Content: map[string]any{
			"description": skill.Description, "version": skill.Version,
			"license": optionalStringValue(skill.License), "compatibility": optionalStringValue(skill.Compatibility),
			"allowed_tools": optionalStringValue(skill.AllowedTools), "body": skill.Body,
			"metadata": skill.Metadata, "source_mode": skill.SourceMode, "enabled": skill.Enabled,
		},
		Metadata: skill.Metadata, Deleted: skill.DeletedAt != nil,
	}
}

func memorySnapshot(memory memorylayersdk.Memory) *refinement.ResourceSnapshot {
	logicalKey := ""
	if memory.LogicalKey != nil {
		logicalKey = *memory.LogicalKey
	}
	return &refinement.ResourceSnapshot{
		ResourceID: memory.ID, ResourceKey: logicalKey, ETag: memory.ETag, SchemaVersion: 1,
		Content: map[string]any{
			"content": memory.Content, "type": memory.Type, "subtype": optionalStringValue(memory.Subtype),
			"tags": memory.Tags, "refinement_metadata": memory.RefinementMetadata, "pinned": memory.Pinned,
		},
		Metadata: memory.Metadata, Deleted: memory.DeletedAt != nil,
	}
}

func optionalStringValue(value *string) any {
	if value == nil {
		return nil
	}
	return *value
}

func mapRefinementMutationError(action string, err error) error {
	var conflict *memorylayersdk.ConflictError
	var precondition *memorylayersdk.PreconditionFailedError
	if errors.As(err, &conflict) || errors.As(err, &precondition) {
		return fmt.Errorf("%w: %s: %v", refinement.ErrConflict, action, err)
	}
	var notFound *memorylayersdk.NotFoundError
	if errors.As(err, &notFound) {
		return fmt.Errorf("%w: %s: %v", refinement.ErrNotFound, action, err)
	}
	var validation *memorylayersdk.ValidationError
	var required *memorylayersdk.PreconditionRequiredError
	if errors.As(err, &validation) || errors.As(err, &required) {
		return fmt.Errorf("%w: %s: %v", refinement.ErrInvalid, action, err)
	}
	return fmt.Errorf("memorylayer: %s: %w", action, err)
}

var _ refinement.ResourceEditor = (*RefinementResourceEditor)(nil)
