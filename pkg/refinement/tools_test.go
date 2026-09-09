// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package refinement

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

func TestRefinementToolsProposeRequireFreshApprovalThenApply(t *testing.T) {
	store := newTestStore(t)
	editor := &recordingEditor{mutation: Mutation{After: &ResourceSnapshot{
		ResourceID: "note-1", ResourceKey: "note", ETag: "e1", Content: map[string]any{"title": "Title", "content": "Body"},
	}}}
	service := &Service{Store: store, Editors: map[ResourceKind]ResourceEditor{ResourcePromptNote: editor}}
	registry := tools.NewRegistry()
	if err := RegisterTools(registry, service); err != nil {
		t.Fatal(err)
	}
	var applyDescriptor tools.Descriptor
	for _, descriptor := range registry.Descriptors() {
		if !json.Valid(descriptor.Parameters) {
			t.Fatalf("invalid schema for %s", descriptor.Name)
		}
		if descriptor.Name == ApplyToolName {
			applyDescriptor = descriptor
		}
	}
	if applyDescriptor.Trust != tools.TrustRequiresFreshApproval {
		t.Fatalf("apply trust = %v", applyDescriptor.Trust)
	}

	plan := applicablePlan()
	arguments, _ := json.Marshal(proposeToolInput{
		Key: "refinements/r1/proposal", RefinementID: plan.RefinementID, Trigger: plan.Trigger, Scope: plan.Scope,
		Summary: plan.Summary, Rationale: plan.Rationale, ExpectedOutcome: plan.ExpectedOutcome, Evidence: plan.Evidence, Edits: plan.Edits,
	})
	address := protocol.MessageAddress{WorkspaceID: "ws-a", TaskID: "task-1"}
	proposed, err := registry.Invoke(context.Background(), tools.Request{CallID: "propose-call", Name: ProposeToolName, Arguments: arguments, Addr: address})
	if err != nil {
		t.Fatal(err)
	}
	var proposal ProposalResult
	if err := json.Unmarshal(proposed.Payload, &proposal); err != nil {
		t.Fatal(err)
	}
	if proposal.Record.TaskRef == nil || proposal.Record.TaskRef.ID != "task-1" || !proposal.Assessment.RequiresApproval {
		t.Fatalf("proposal = %+v", proposal)
	}

	applyArguments, _ := json.Marshal(map[string]string{"proposal_record_id": proposal.Record.ID})
	_, err = registry.Invoke(context.Background(), tools.Request{CallID: "apply-call", Name: ApplyToolName, Arguments: applyArguments, Addr: address})
	if !errors.Is(err, tools.ErrToolRequiresApproval) || len(editor.operations) != 0 {
		t.Fatalf("unapproved apply err=%v operations=%v", err, editor.operations)
	}
	applied, err := registry.Invoke(context.Background(), tools.Request{
		CallID: "apply-call", Name: ApplyToolName, Arguments: applyArguments, Addr: address, Approved: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	var application ApplyResult
	if err := json.Unmarshal(applied.Payload, &application); err != nil {
		t.Fatal(err)
	}
	if application.Application == nil || application.Application.Outcome != OutcomeApplied || application.Application.ApprovalRef == nil || application.Application.ApprovalRef.ID != "apply-call" {
		t.Fatalf("application = %+v", application)
	}

	rollbackArguments, _ := json.Marshal(map[string]string{
		"application_record_id": application.Application.ID, "key": "refinements/r1/rollback", "refinement_id": "rollback-1",
	})
	rollbackResult, err := registry.Invoke(context.Background(), tools.Request{CallID: "rollback-call", Name: PlanRollbackToolName, Arguments: rollbackArguments, Addr: address})
	if err != nil {
		t.Fatal(err)
	}
	var rollback ProposalResult
	if err := json.Unmarshal(rollbackResult.Payload, &rollback); err != nil {
		t.Fatal(err)
	}
	if rollback.Record.RollbackOfRecordID != application.Application.ID || rollback.Assessment.Risk != RiskHigh || !rollback.Assessment.RequiresApproval {
		t.Fatalf("rollback proposal = %+v", rollback)
	}

	queryArguments, _ := json.Marshal(map[string]any{"outcomes": []string{"applied"}, "resource_kinds": []string{"prompt_note"}, "text": "instruction", "limit": 1})
	queryResult, err := registry.Invoke(context.Background(), tools.Request{
		CallID: "query-call", Name: QueryRecordsToolName, Arguments: queryArguments, Addr: address,
	})
	if err != nil {
		t.Fatal(err)
	}
	var page Page
	if err := json.Unmarshal(queryResult.Payload, &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Records) != 1 || page.Records[0].ID != application.Application.ID {
		t.Fatalf("query page = %+v", page)
	}
}

func TestRefinementApplyToolCanAutoApplyOptedInLowRiskSessionEdit(t *testing.T) {
	store := newTestStore(t)
	editor := &recordingEditor{mutation: Mutation{After: &ResourceSnapshot{ResourceID: "note-1", ResourceKey: "note", ETag: "e1"}}}
	service := &Service{
		Store: store, Editors: map[ResourceKind]ResourceEditor{ResourcePromptNote: editor},
		Policy: Policy{AllowSessionLowRiskWithoutApproval: true},
	}
	proposal, err := service.Propose(context.Background(), "ws", ProposeRequest{OperationID: "proposal", Key: "proposal/key", Plan: applicablePlan()})
	if err != nil {
		t.Fatal(err)
	}
	registry := tools.NewRegistry()
	if err := RegisterTools(registry, service); err != nil {
		t.Fatal(err)
	}
	arguments, _ := json.Marshal(map[string]string{"proposal_record_id": proposal.Record.ID})
	result, err := registry.Invoke(context.Background(), tools.Request{
		CallID: "apply-low", Name: ApplyToolName, Arguments: arguments, Addr: protocol.MessageAddress{WorkspaceID: "ws"},
	})
	if err != nil {
		t.Fatal(err)
	}
	var applied ApplyResult
	if err := json.Unmarshal(result.Payload, &applied); err != nil {
		t.Fatal(err)
	}
	if applied.Application == nil || applied.Application.ApprovalRef != nil || applied.Assessment.RequiresApproval {
		t.Fatalf("auto application = %+v", applied)
	}
}
