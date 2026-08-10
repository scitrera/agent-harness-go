package memorylayer

import (
	"context"
	"fmt"

	memorylayersdk "github.com/scitrera/memorylayer/memorylayer-sdk-go"

	"github.com/scitrera/agent-harness-go/pkg/refinement"
)

const (
	refinementRecordPageSize = 100
	maxRefinementRecordPages = 20
)

// RefinementRecordStore adapts the neutral append-only refinement.Store to
// MemoryLayer's typed resource API. It does not apply proposals or mirror live
// Aether task state.
type RefinementRecordStore struct {
	client *memorylayersdk.Client
}

type refinementRecordStoreConfig struct {
	transport memorylayersdk.Transport
}

type RefinementRecordStoreOption func(*refinementRecordStoreConfig)

func WithRefinementRecordTransport(transport memorylayersdk.Transport) RefinementRecordStoreOption {
	return func(config *refinementRecordStoreConfig) { config.transport = transport }
}

func NewRefinementRecordStore(cfg Config, storeOptions ...RefinementRecordStoreOption) (*RefinementRecordStore, error) {
	storeConfig := refinementRecordStoreConfig{}
	for _, option := range storeOptions {
		if option != nil {
			option(&storeConfig)
		}
	}
	transport := storeConfig.transport
	if transport == nil {
		transport = cfg.Transport
	}
	opts := []memorylayersdk.Option{
		memorylayersdk.WithAPIKey(cfg.APIKey),
		memorylayersdk.WithWorkspaceID(cfg.Workspace),
	}
	if transport != nil {
		opts = append(opts, memorylayersdk.WithTransport(transport))
	} else {
		if cfg.BaseURL == "" {
			return nil, fmt.Errorf("memorylayer: refinement records base url or transport required")
		}
		opts = append(opts, memorylayersdk.WithBaseURL(cfg.BaseURL))
	}
	if cfg.HTTPClient != nil && transport == nil {
		opts = append(opts, memorylayersdk.WithHTTPClient(cfg.HTTPClient))
	}
	client, err := memorylayersdk.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("memorylayer: refinement records client: %w", err)
	}
	return &RefinementRecordStore{client: client}, nil
}

func (s *RefinementRecordStore) Append(ctx context.Context, workspaceID, operationID string, request refinement.AppendRequest) (refinement.AppendResult, error) {
	if request.SchemaVersion == 0 {
		request.SchemaVersion = 1
	}
	if err := request.Validate(); err != nil {
		return refinement.AppendResult{}, err
	}
	result, err := s.client.RefinementRecords.Create(ctx, refinementCreateInput(request), memorylayersdk.RefinementRecordCreateOptions{
		WorkspaceID: workspaceID, IdempotencyKey: operationID, Authority: promptNoteAuthority(ctx),
	})
	if err != nil {
		return refinement.AppendResult{}, fmt.Errorf("memorylayer: append refinement record: %w", err)
	}
	return refinement.AppendResult{Record: refinementRecord(result.Record), Replayed: result.Replayed}, nil
}

func (s *RefinementRecordStore) Get(ctx context.Context, workspaceID, recordID string) (refinement.Record, error) {
	record, err := s.client.RefinementRecords.Get(ctx, recordID, memorylayersdk.RefinementRecordGetOptions{
		WorkspaceID: workspaceID, Authority: promptNoteAuthority(ctx),
	})
	if err != nil {
		return refinement.Record{}, fmt.Errorf("memorylayer: get refinement record: %w", err)
	}
	return refinementRecord(*record), nil
}

func (s *RefinementRecordStore) List(ctx context.Context, workspaceID string) ([]refinement.Record, error) {
	pageToken := ""
	seenTokens := map[string]struct{}{}
	var records []refinement.Record
	for pageNumber := 0; pageNumber < maxRefinementRecordPages; pageNumber++ {
		page, err := s.client.RefinementRecords.List(ctx, memorylayersdk.RefinementRecordListOptions{
			WorkspaceID: workspaceID, Limit: refinementRecordPageSize, PageToken: pageToken, Authority: promptNoteAuthority(ctx),
		})
		if err != nil {
			return nil, fmt.Errorf("memorylayer: list refinement records: %w", err)
		}
		for _, record := range page.Records {
			records = append(records, refinementRecord(record))
		}
		if page.NextPageToken == "" {
			return records, nil
		}
		if _, repeated := seenTokens[page.NextPageToken]; repeated {
			return nil, fmt.Errorf("memorylayer: refinement-record cursor cycle at %q", page.NextPageToken)
		}
		seenTokens[page.NextPageToken] = struct{}{}
		pageToken = page.NextPageToken
	}
	return nil, fmt.Errorf("memorylayer: refinement-record listing exceeds %d pages (%d records maximum)", maxRefinementRecordPages, refinementRecordPageSize*maxRefinementRecordPages)
}

func refinementCreateInput(request refinement.AppendRequest) memorylayersdk.RefinementRecordCreateInput {
	return memorylayersdk.RefinementRecordCreateInput{
		Key: request.Key, RefinementID: request.RefinementID, Phase: string(request.Phase), Trigger: request.Trigger,
		Scope: string(request.Scope), Summary: request.Summary, Rationale: request.Rationale, ExpectedOutcome: request.ExpectedOutcome,
		Evidence:           sdkRefinementEvidence(request.Evidence),
		Edits:              sdkRefinementEdits(request.Edits),
		Outcome:            string(request.Outcome),
		ParentRecordID:     optionalString(request.ParentRecordID),
		RollbackOfRecordID: optionalString(request.RollbackOfRecordID),
		TaskRef:            sdkExternalReference(request.TaskRef),
		ApprovalRef:        sdkExternalReference(request.ApprovalRef),
		SchemaVersion:      request.SchemaVersion,
		Metadata:           request.Metadata,
	}
}

func sdkRefinementEvidence(evidence []refinement.Evidence) []memorylayersdk.RefinementEvidence {
	out := make([]memorylayersdk.RefinementEvidence, len(evidence))
	for i, item := range evidence {
		out[i] = memorylayersdk.RefinementEvidence{
			Kind: string(item.Kind), Reference: item.Reference, Description: item.Description, ContentHash: optionalString(item.ContentHash),
		}
	}
	return out
}

func sdkRefinementEdits(edits []refinement.Edit) []memorylayersdk.RefinementEdit {
	out := make([]memorylayersdk.RefinementEdit, len(edits))
	for i, edit := range edits {
		out[i] = memorylayersdk.RefinementEdit{
			Action: string(edit.Action), ResourceKind: string(edit.ResourceKind), ResourceKey: edit.ResourceKey,
			ResourceID: optionalString(edit.ResourceID), ExpectedETag: optionalString(edit.ExpectedETag),
			BeforeETag: optionalString(edit.BeforeETag), AfterETag: optionalString(edit.AfterETag),
			Reason: edit.Reason, Content: edit.Content, Before: sdkResourceSnapshot(edit.Before), After: sdkResourceSnapshot(edit.After),
			Applied: edit.Applied, Error: optionalString(edit.Error),
		}
	}
	return out
}

func sdkResourceSnapshot(snapshot *refinement.ResourceSnapshot) *memorylayersdk.RefinementResourceSnapshot {
	if snapshot == nil {
		return nil
	}
	return &memorylayersdk.RefinementResourceSnapshot{
		ResourceID: optionalString(snapshot.ResourceID), ResourceKey: snapshot.ResourceKey, ETag: optionalString(snapshot.ETag),
		SchemaVersion: snapshot.SchemaVersion, Content: snapshot.Content, Metadata: snapshot.Metadata, Deleted: snapshot.Deleted,
	}
}

func sdkExternalReference(reference *refinement.ExternalReference) *memorylayersdk.RefinementExternalReference {
	if reference == nil {
		return nil
	}
	return &memorylayersdk.RefinementExternalReference{System: reference.System, ID: reference.ID, AttemptID: optionalString(reference.AttemptID)}
}

func refinementRecord(record memorylayersdk.RefinementRecord) refinement.Record {
	return refinement.Record{
		ID: record.ID, WorkspaceID: record.WorkspaceID,
		AppendRequest: refinement.AppendRequest{
			Key: record.Key,
			Plan: refinement.Plan{
				RefinementID: record.RefinementID, Trigger: record.Trigger, Scope: refinement.Scope(record.Scope),
				Summary: record.Summary, Rationale: record.Rationale, ExpectedOutcome: record.ExpectedOutcome,
				Evidence: refinementEvidence(record.Evidence), Edits: refinementEdits(record.Edits),
			},
			Phase: refinement.Phase(record.Phase), Outcome: refinement.Outcome(record.Outcome),
			ParentRecordID: valueString(record.ParentRecordID), RollbackOfRecordID: valueString(record.RollbackOfRecordID),
			TaskRef: externalReference(record.TaskRef), ApprovalRef: externalReference(record.ApprovalRef),
			SchemaVersion: record.SchemaVersion, Metadata: record.Metadata,
		},
		Revision: record.Revision, ETag: record.ETag, CreatedAt: record.CreatedAt,
	}
}

func refinementEvidence(evidence []memorylayersdk.RefinementEvidence) []refinement.Evidence {
	out := make([]refinement.Evidence, len(evidence))
	for i, item := range evidence {
		out[i] = refinement.Evidence{Kind: refinement.EvidenceKind(item.Kind), Reference: item.Reference, Description: item.Description, ContentHash: valueString(item.ContentHash)}
	}
	return out
}

func refinementEdits(edits []memorylayersdk.RefinementEdit) []refinement.Edit {
	out := make([]refinement.Edit, len(edits))
	for i, edit := range edits {
		out[i] = refinement.Edit{
			Action: refinement.Action(edit.Action), ResourceKind: refinement.ResourceKind(edit.ResourceKind), ResourceKey: edit.ResourceKey,
			ResourceID: valueString(edit.ResourceID), ExpectedETag: valueString(edit.ExpectedETag), BeforeETag: valueString(edit.BeforeETag),
			AfterETag: valueString(edit.AfterETag), Reason: edit.Reason, Content: edit.Content,
			Before: resourceSnapshot(edit.Before), After: resourceSnapshot(edit.After), Applied: edit.Applied, Error: valueString(edit.Error),
		}
	}
	return out
}

func resourceSnapshot(snapshot *memorylayersdk.RefinementResourceSnapshot) *refinement.ResourceSnapshot {
	if snapshot == nil {
		return nil
	}
	return &refinement.ResourceSnapshot{
		ResourceID: valueString(snapshot.ResourceID), ResourceKey: snapshot.ResourceKey, ETag: valueString(snapshot.ETag),
		SchemaVersion: snapshot.SchemaVersion, Content: snapshot.Content, Metadata: snapshot.Metadata, Deleted: snapshot.Deleted,
	}
}

func externalReference(reference *memorylayersdk.RefinementExternalReference) *refinement.ExternalReference {
	if reference == nil {
		return nil
	}
	return &refinement.ExternalReference{System: reference.System, ID: reference.ID, AttemptID: valueString(reference.AttemptID)}
}

func optionalString(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func valueString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

var _ refinement.Store = (*RefinementRecordStore)(nil)
