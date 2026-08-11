package refinement

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/scitrera/agent-harness-go/pkg/tools"
)

const (
	ProposeToolName      = "propose_refinement"
	ApplyToolName        = "apply_refinement"
	PlanRollbackToolName = "plan_refinement_rollback"
	GetRecordToolName    = "get_refinement_record"
	QueryRecordsToolName = "query_refinement_records"
)

func RegisterTools(registry *tools.Registry, service *Service) error {
	if registry == nil || service == nil || service.Store == nil {
		return errors.New("refinement: tool registration requires registry and service")
	}
	registrations := []struct {
		name       string
		handler    tools.Handler
		descriptor tools.Descriptor
	}{
		{ProposeToolName, tools.HandlerFunc(service.proposeTool), tools.Descriptor{
			Name:        ProposeToolName,
			Description: "Append an evidence-backed, conflict-checkable proposal to the workspace refinement audit. This does not mutate the target resources.",
			Parameters:  json.RawMessage(`{"type":"object","properties":{"key":{"type":"string"},"refinement_id":{"type":"string"},"trigger":{"type":"string"},"scope":{"type":"string","enum":["session","workspace","user","tenant","global"]},"summary":{"type":"string"},"rationale":{"type":"string"},"expected_outcome":{"type":"string"},"evidence":{"type":"array","items":{"type":"object","properties":{"kind":{"type":"string","enum":["message","tool_result","resource_revision","evaluation","user_instruction","other"]},"reference":{"type":"string"},"description":{"type":"string"},"content_hash":{"type":"string"}},"required":["kind","reference","description"]}},"edits":{"type":"array","maxItems":32,"items":{"type":"object","properties":{"action":{"type":"string","enum":["create","replace","delete","restore"]},"resource_kind":{"type":"string","enum":["prompt_note","memory","skill","agent_specification"]},"resource_key":{"type":"string"},"resource_id":{"type":"string"},"expected_etag":{"type":"string"},"reason":{"type":"string"},"content":{"type":"object"}},"required":["action","resource_kind","resource_key","reason"]}},"metadata":{"type":"object"}},"required":["key","refinement_id","trigger","scope","summary","rationale","expected_outcome","evidence","edits"]}`),
		}},
		{ApplyToolName, tools.HandlerFunc(service.applyTool), tools.Descriptor{
			Name:        ApplyToolName,
			Description: "Apply one recorded refinement proposal using its exact ETag preconditions. Non-session or elevated-risk changes require a fresh once-only approval; retries are idempotent.",
			Parameters:  json.RawMessage(`{"type":"object","properties":{"proposal_record_id":{"type":"string"}},"required":["proposal_record_id"]}`),
			Trust:       tools.TrustRequiresFreshApproval,
		}},
		{PlanRollbackToolName, tools.HandlerFunc(service.planRollbackTool), tools.Descriptor{
			Name:        PlanRollbackToolName,
			Description: "Create a high-risk rollback proposal from the before/after snapshots in an application record. Applying that new proposal still requires fresh approval.",
			Parameters:  json.RawMessage(`{"type":"object","properties":{"application_record_id":{"type":"string"},"key":{"type":"string"},"refinement_id":{"type":"string"}},"required":["application_record_id","key","refinement_id"]}`),
		}},
		{GetRecordToolName, tools.HandlerFunc(service.getRecordTool), tools.Descriptor{
			Name:        GetRecordToolName,
			Description: "Read one immutable refinement proposal, decision, application, or rollback audit record in the current workspace.",
			Parameters:  json.RawMessage(`{"type":"object","properties":{"record_id":{"type":"string"}},"required":["record_id"]}`),
		}},
		{QueryRecordsToolName, tools.HandlerFunc(service.queryRecordsTool), tools.Descriptor{
			Name:        QueryRecordsToolName,
			Description: "Search one bounded newest-first page of the authoritative refinement audit. Preserve the returned opaque cursor exactly; use failed or partially_applied outcomes to find immutable records needing operator attention.",
			Parameters:  json.RawMessage(`{"type":"object","properties":{"phases":{"type":"array","maxItems":16,"items":{"type":"string","enum":["proposal","decision","application","rollback"]}},"outcomes":{"type":"array","maxItems":16,"items":{"type":"string","enum":["proposed","approved","rejected","applied","partially_applied","failed","rolled_back","no_op"]}},"scopes":{"type":"array","maxItems":16,"items":{"type":"string","enum":["session","workspace","user","tenant","global"]}},"resource_kinds":{"type":"array","maxItems":16,"items":{"type":"string","enum":["prompt_note","memory","skill","agent_specification"]}},"refinement_id":{"type":"string","maxLength":200},"text":{"type":"string","maxLength":200},"limit":{"type":"integer","minimum":1,"maximum":100},"cursor":{"type":"string","maxLength":8192}}}`),
		}},
	}
	for _, registration := range registrations {
		if err := registry.Register(registration.name, registration.handler); err != nil {
			return err
		}
		registry.Describe(registration.descriptor)
	}
	return nil
}

type proposeToolInput struct {
	Key             string         `json:"key"`
	RefinementID    string         `json:"refinement_id"`
	Trigger         string         `json:"trigger"`
	Scope           Scope          `json:"scope"`
	Summary         string         `json:"summary"`
	Rationale       string         `json:"rationale"`
	ExpectedOutcome string         `json:"expected_outcome"`
	Evidence        []Evidence     `json:"evidence"`
	Edits           []Edit         `json:"edits"`
	Metadata        map[string]any `json:"metadata"`
}

func (s *Service) proposeTool(ctx context.Context, request tools.Request) (tools.Result, error) {
	var input proposeToolInput
	if err := decodeRefinementToolArgs(request, &input); err != nil {
		return tools.Result{}, err
	}
	result, err := s.Propose(ctx, request.Addr.WorkspaceID, ProposeRequest{
		OperationID: derivedOperationID(request.CallID, "proposal"), Key: input.Key,
		Plan: Plan{
			RefinementID: input.RefinementID, Trigger: input.Trigger, Scope: input.Scope, Summary: input.Summary,
			Rationale: input.Rationale, ExpectedOutcome: input.ExpectedOutcome, Evidence: input.Evidence, Edits: input.Edits,
		},
		TaskRef: taskReference(request), Metadata: input.Metadata,
	})
	if err != nil {
		return tools.Result{}, err
	}
	return refinementToolResult(request, result)
}

func (s *Service) applyTool(ctx context.Context, request tools.Request) (tools.Result, error) {
	var input struct {
		ProposalRecordID string `json:"proposal_record_id"`
	}
	if err := decodeRefinementToolArgs(request, &input); err != nil {
		return tools.Result{}, err
	}
	approval := Approval{Granted: true}
	if request.Approved {
		approval.Reference = &ExternalReference{System: "ecosystem-approval", ID: request.CallID, AttemptID: request.Addr.TaskID}
	}
	result, err := s.Apply(ctx, request.Addr.WorkspaceID, ApplyRequest{
		OperationID: derivedOperationID(request.CallID, "apply"), ProposalRecordID: strings.TrimSpace(input.ProposalRecordID), Approval: approval,
	})
	if errors.Is(err, ErrApprovalRequired) {
		return tools.Result{}, fmt.Errorf("%w: review the exact refinement proposal before applying it", tools.ErrToolRequiresApproval)
	}
	if err != nil {
		return tools.Result{}, err
	}
	return refinementToolResult(request, result)
}

func (s *Service) planRollbackTool(ctx context.Context, request tools.Request) (tools.Result, error) {
	var input struct {
		ApplicationRecordID string `json:"application_record_id"`
		Key                 string `json:"key"`
		RefinementID        string `json:"refinement_id"`
	}
	if err := decodeRefinementToolArgs(request, &input); err != nil {
		return tools.Result{}, err
	}
	application, err := s.Store.Get(ctx, request.Addr.WorkspaceID, strings.TrimSpace(input.ApplicationRecordID))
	if err != nil {
		return tools.Result{}, err
	}
	plan, err := BuildRollbackPlan(application, strings.TrimSpace(input.RefinementID))
	if err != nil {
		return tools.Result{}, err
	}
	result, err := s.Propose(ctx, request.Addr.WorkspaceID, ProposeRequest{
		OperationID: derivedOperationID(request.CallID, "rollback-proposal"), Key: input.Key, Plan: plan,
		RollbackOfRecordID: application.ID, TaskRef: taskReference(request),
	})
	if err != nil {
		return tools.Result{}, err
	}
	return refinementToolResult(request, result)
}

func (s *Service) getRecordTool(ctx context.Context, request tools.Request) (tools.Result, error) {
	var input struct {
		RecordID string `json:"record_id"`
	}
	if err := decodeRefinementToolArgs(request, &input); err != nil {
		return tools.Result{}, err
	}
	record, err := s.Store.Get(ctx, request.Addr.WorkspaceID, strings.TrimSpace(input.RecordID))
	if err != nil {
		return tools.Result{}, err
	}
	return refinementToolResult(request, record)
}

func (s *Service) queryRecordsTool(ctx context.Context, request tools.Request) (tools.Result, error) {
	var input struct {
		Phases        []Phase        `json:"phases"`
		Outcomes      []Outcome      `json:"outcomes"`
		Scopes        []Scope        `json:"scopes"`
		ResourceKinds []ResourceKind `json:"resource_kinds"`
		RefinementID  string         `json:"refinement_id"`
		Text          string         `json:"text"`
		Limit         int            `json:"limit"`
		Cursor        string         `json:"cursor"`
	}
	if err := decodeRefinementToolArgs(request, &input); err != nil {
		return tools.Result{}, err
	}
	page, err := s.Store.Query(ctx, request.Addr.WorkspaceID, Query{
		Phases: input.Phases, Outcomes: input.Outcomes, Scopes: input.Scopes, ResourceKinds: input.ResourceKinds,
		RefinementID: input.RefinementID, Text: input.Text, Limit: input.Limit, PageToken: input.Cursor,
	})
	if err != nil {
		return tools.Result{}, err
	}
	return refinementToolResult(request, page)
}

func taskReference(request tools.Request) *ExternalReference {
	if strings.TrimSpace(request.Addr.TaskID) == "" {
		return nil
	}
	return &ExternalReference{System: "aether", ID: request.Addr.TaskID}
}

func decodeRefinementToolArgs(request tools.Request, target any) error {
	if len(request.Arguments) == 0 {
		return fmt.Errorf("%w: arguments are required", ErrInvalid)
	}
	decoder := json.NewDecoder(strings.NewReader(string(request.Arguments)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("%w: invalid tool arguments: %v", ErrInvalid, err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("%w: multiple argument documents", ErrInvalid)
		}
		return fmt.Errorf("%w: invalid trailing tool arguments: %v", ErrInvalid, err)
	}
	return nil
}

func refinementToolResult(request tools.Request, value any) (tools.Result, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return tools.Result{}, err
	}
	result, err := tools.NewJSONResult(request.CallID, request.Name, payload)
	if err != nil {
		return tools.Result{}, err
	}
	for _, record := range refinementRecords(value) {
		if record.ID != "" {
			result.Metadata.References = append(result.Metadata.References, tools.ResultReference{System: "refinement-store", Kind: "refinement_record", ID: record.ID})
		}
	}
	return result, nil
}

func refinementRecords(value any) []Record {
	switch typed := value.(type) {
	case Record:
		return []Record{typed}
	case ProposalResult:
		return []Record{typed.Record}
	case ApplyResult:
		records := []Record{typed.Decision}
		if typed.Application != nil {
			records = append(records, *typed.Application)
		}
		return records
	case Page:
		return typed.Records
	default:
		return nil
	}
}
