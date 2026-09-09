// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package refinement

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestAssessIsScopeAndContentSensitive(t *testing.T) {
	plan := applicablePlan()
	assessment := Assess(plan, Policy{AllowSessionLowRiskWithoutApproval: true})
	if assessment.Risk != RiskLow || assessment.RequiresApproval {
		t.Fatalf("session prompt-note assessment = %+v", assessment)
	}

	plan.Scope = ScopeWorkspace
	assessment = Assess(plan, Policy{AllowSessionLowRiskWithoutApproval: true})
	if assessment.Risk != RiskMedium || !assessment.RequiresApproval {
		t.Fatalf("workspace assessment = %+v", assessment)
	}

	plan.Scope = ScopeSession
	plan.Edits[0].ResourceKind = ResourceAgentSpecification
	plan.Edits[0].Content["allowed_tools"] = []any{"shell"}
	assessment = Assess(plan, Policy{AllowSessionLowRiskWithoutApproval: true})
	if assessment.Risk != RiskHigh || !assessment.RequiresApproval {
		t.Fatalf("capability-bearing assessment = %+v", assessment)
	}
}

func TestServiceApplyRequiresApprovalAndRecordsAuthorityResult(t *testing.T) {
	store := newTestStore(t)
	editor := &recordingEditor{mutation: Mutation{
		After: &ResourceSnapshot{ResourceID: "note-1", ResourceKey: "note", ETag: `"v1"`, Content: map[string]any{"title": "Title", "content": "Body"}},
	}}
	service := &Service{Store: store, Editors: map[ResourceKind]ResourceEditor{ResourcePromptNote: editor}}
	proposal, err := service.Propose(context.Background(), "ws-a", ProposeRequest{
		OperationID: "proposal-op", Key: "refinement/proposal", Plan: applicablePlan(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !proposal.Assessment.RequiresApproval {
		t.Fatal("safe default must require approval")
	}

	_, err = service.Apply(context.Background(), "ws-a", ApplyRequest{
		OperationID: "apply-op", ProposalRecordID: proposal.Record.ID, Approval: Approval{Granted: true},
	})
	if !errors.Is(err, ErrApprovalRequired) {
		t.Fatalf("missing approval error = %v", err)
	}

	result, err := service.Apply(context.Background(), "ws-a", ApplyRequest{
		OperationID: "apply-op", ProposalRecordID: proposal.Record.ID,
		Approval: Approval{Granted: true, Reference: &ExternalReference{System: "aether", ID: "approval-1", AttemptID: "attempt-1"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Decision.Outcome != OutcomeApproved || result.Application == nil || result.Application.Outcome != OutcomeApplied {
		t.Fatalf("apply result = %+v", result)
	}
	edit := result.Application.Edits[0]
	if edit.Applied == nil || !*edit.Applied || edit.AfterETag != `"v1"` || edit.After == nil {
		t.Fatalf("application edit = %+v", edit)
	}
	if len(editor.operations) != 1 || editor.operations[0] == "apply-op" {
		t.Fatalf("editor operations = %v", editor.operations)
	}
	page, err := store.Query(context.Background(), "ws-a", Query{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	records := page.Records
	if len(records) != 3 {
		t.Fatalf("record count = %d, want proposal + decision + application", len(records))
	}
	if records[0].ParentRecordID != records[1].ID || records[1].ParentRecordID != records[2].ID {
		t.Fatalf("audit linkage newest-first = %+v", records)
	}
}

func TestServiceRejectedDecisionHasNoSideEffects(t *testing.T) {
	store := newTestStore(t)
	editor := &recordingEditor{}
	service := &Service{Store: store, Editors: map[ResourceKind]ResourceEditor{ResourcePromptNote: editor}}
	proposal, err := service.Propose(context.Background(), "ws-a", ProposeRequest{
		OperationID: "proposal-op", Key: "refinement/proposal", Plan: applicablePlan(),
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.Apply(context.Background(), "ws-a", ApplyRequest{
		OperationID: "reject-op", ProposalRecordID: proposal.Record.ID,
		Approval: Approval{Granted: false, Reference: &ExternalReference{System: "local", ID: "decision-1"}},
	})
	if !errors.Is(err, ErrApprovalRejected) || result.Decision.Outcome != OutcomeRejected {
		t.Fatalf("rejection result = %+v, err = %v", result, err)
	}
	if len(editor.operations) != 0 || result.Application != nil {
		t.Fatalf("rejected request mutated resources: %+v", result)
	}
}

func TestServiceFailClosedForUnsupportedResource(t *testing.T) {
	store := newTestStore(t)
	service := &Service{Store: store}
	plan := applicablePlan()
	plan.Edits[0].ResourceKind = ResourceSkill
	proposal, err := service.Propose(context.Background(), "ws-a", ProposeRequest{
		OperationID: "proposal-op", Key: "refinement/proposal", Plan: plan,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.Apply(context.Background(), "ws-a", ApplyRequest{
		OperationID: "apply-op", ProposalRecordID: proposal.Record.ID,
		Approval: Approval{Granted: true, Reference: &ExternalReference{System: "local", ID: "approval-1"}},
	})
	if !errors.Is(err, ErrUnsupportedResource) || result.Application == nil || result.Application.Outcome != OutcomeFailed {
		t.Fatalf("unsupported result = %+v, err = %v", result, err)
	}
	if result.Application.Edits[0].Applied == nil || *result.Application.Edits[0].Applied {
		t.Fatalf("unsupported edit = %+v", result.Application.Edits[0])
	}
}

func TestBuildRollbackPlanReversesAppliedEditsWithCurrentETags(t *testing.T) {
	applied := true
	application := Record{
		ID: "application-1",
		AppendRequest: AppendRequest{
			Plan: Plan{
				RefinementID: "original", Trigger: "test", Scope: ScopeWorkspace, Summary: "change resources",
				Rationale: "test", ExpectedOutcome: "changed",
				Evidence: []Evidence{{Kind: EvidenceMessage, Reference: "m1", Description: "test"}},
				Edits: []Edit{
					{Action: ActionCreate, ResourceKind: ResourcePromptNote, ResourceKey: "created", ResourceID: "n1", Applied: &applied, After: &ResourceSnapshot{ResourceID: "n1", ResourceKey: "created", ETag: `"n1-v1"`}},
					{Action: ActionReplace, ResourceKind: ResourceAgentSpecification, ResourceKey: "reviewer", ResourceID: "a1", Applied: &applied, Before: &ResourceSnapshot{ResourceID: "a1", ResourceKey: "reviewer", ETag: `"a1-v1"`, Content: map[string]any{"name": "old"}}, After: &ResourceSnapshot{ResourceID: "a1", ResourceKey: "reviewer", ETag: `"a1-v2"`, Content: map[string]any{"name": "new"}}},
				},
			},
			Phase: PhaseApplication, Outcome: OutcomeApplied,
		},
	}
	plan, err := BuildRollbackPlan(application, "rollback-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Edits) != 2 || plan.Edits[0].Action != ActionReplace || plan.Edits[1].Action != ActionDelete {
		t.Fatalf("rollback order/actions = %+v", plan.Edits)
	}
	if plan.Edits[0].ExpectedETag != `"a1-v2"` || !reflect.DeepEqual(plan.Edits[0].Content, map[string]any{"name": "old"}) {
		t.Fatalf("replacement rollback = %+v", plan.Edits[0])
	}
	if plan.Edits[1].ExpectedETag != `"n1-v1"` || plan.Edits[1].ResourceID != "n1" {
		t.Fatalf("create rollback = %+v", plan.Edits[1])
	}
}

func TestProposeRejectsUnsafeMutationShape(t *testing.T) {
	service := &Service{Store: newTestStore(t)}
	plan := applicablePlan()
	plan.Edits[0].Action = ActionReplace
	if _, err := service.Propose(context.Background(), "ws", ProposeRequest{OperationID: "op", Key: "key", Plan: plan}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("replace without ETag error = %v", err)
	}
}

func TestBuildRollbackPlanUsesSchemaV2NativeRestoreForDeletedResources(t *testing.T) {
	applied := true
	application := Record{
		ID: "application-1",
		AppendRequest: AppendRequest{
			Plan: Plan{Scope: ScopeWorkspace, Summary: "delete note", Edits: []Edit{{
				Action: ActionDelete, ResourceKind: ResourcePromptNote, ResourceKey: "note", Applied: &applied,
				Before: &ResourceSnapshot{ResourceID: "note-1", ResourceKey: "note", ETag: "e1", Content: map[string]any{"title": "Old"}},
				After:  &ResourceSnapshot{ResourceID: "note-1", ResourceKey: "note", ETag: "e2", Content: map[string]any{"title": "Old"}, Deleted: true},
			}}},
			Phase: PhaseApplication, Outcome: OutcomeApplied,
		},
	}
	plan, err := BuildRollbackPlan(application, "rollback-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Edits) != 1 || plan.Edits[0].Action != ActionRestore || plan.Edits[0].ResourceID != "note-1" || plan.Edits[0].ExpectedETag != "e2" {
		t.Fatalf("restore rollback = %+v", plan.Edits)
	}
	service := &Service{Store: newTestStore(t)}
	proposal, err := service.Propose(context.Background(), "ws", ProposeRequest{
		OperationID: "rollback-proposal", Key: "rollback/proposal", Plan: plan, RollbackOfRecordID: application.ID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if proposal.Record.SchemaVersion != 2 || proposal.Assessment.Risk != RiskHigh || !proposal.Assessment.RequiresApproval {
		t.Fatalf("restore proposal = %+v", proposal)
	}
}

type recordingEditor struct {
	operations []string
	mutation   Mutation
	err        error
}

func (e *recordingEditor) Apply(_ context.Context, _ string, operationID string, _ Edit) (Mutation, error) {
	e.operations = append(e.operations, operationID)
	return e.mutation, e.err
}

func applicablePlan() Plan {
	return Plan{
		RefinementID:    "refinement-1",
		Trigger:         "user feedback",
		Scope:           ScopeSession,
		Summary:         "add a prompt note",
		Rationale:       "the instruction should remain visible",
		ExpectedOutcome: "future turns include the note",
		Evidence:        []Evidence{{Kind: EvidenceUserInstruction, Reference: "message-1", Description: "user requested the behavior"}},
		Edits: []Edit{{
			Action: ActionCreate, ResourceKind: ResourcePromptNote, ResourceKey: "note", Reason: "capture the instruction",
			Content: map[string]any{"title": "Title", "content": "Body"},
		}},
	}
}

func newTestStore(t *testing.T) *FileStore {
	t.Helper()
	store, err := NewFileStore(filepath.Join(t.TempDir(), "state"), func() time.Time {
		return time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	})
	if err != nil {
		t.Fatal(err)
	}
	return store
}
