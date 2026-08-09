package goal

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/tools"
)

const (
	CreateToolName = "create_goal"
	GetToolName    = "get_goal"
	UpdateToolName = "update_goal"
)

// RegisterTools exposes the portable goal service to the model. Creation is
// deliberately described as user-directed; calling the tool does not grant new
// authority beyond the current turn.
func RegisterTools(registry *tools.Registry, service *Service) error {
	if registry == nil || service == nil {
		return errors.New("goal: tool registration requires registry and service")
	}
	registrations := []struct {
		name       string
		handler    tools.Handler
		descriptor tools.Descriptor
	}{
		{CreateToolName, tools.HandlerFunc(service.createTool), tools.Descriptor{
			Name:        CreateToolName,
			Description: "Create one durable goal for this workspace/session only when the user explicitly requests persistent multi-turn work. Fails while another goal is open.",
			Parameters:  json.RawMessage(`{"type":"object","properties":{"objective":{"type":"string","description":"The complete user-requested objective"},"token_budget":{"type":"integer","minimum":1,"description":"Optional cumulative provider-token budget"}},"required":["objective"]}`),
		}},
		{GetToolName, tools.HandlerFunc(service.getTool), tools.Descriptor{
			Name:        GetToolName,
			Description: "Read the current durable goal and remaining token budget, or fetch a historical goal by ID.",
			Parameters:  json.RawMessage(`{"type":"object","properties":{"id":{"type":"string","description":"Optional stable goal ID; omitted returns the current/latest goal"}}}`),
		}},
		{UpdateToolName, tools.HandlerFunc(service.updateTool), tools.Descriptor{
			Name:        UpdateToolName,
			Description: "Explicitly complete, block, cancel, or resume the open durable goal. Complete only after auditing the objective and include stable evidence references when available; never complete merely because a budget is nearly exhausted.",
			Parameters:  json.RawMessage(`{"type":"object","properties":{"id":{"type":"string","description":"Optional goal ID; omitted selects the open goal"},"status":{"type":"string","enum":["active","completed","blocked","cancelled"]},"evidence":{"type":"array","items":{"type":"string"},"description":"Stable message, artifact, task, or verifier references"},"blocked_reason":{"type":"string","description":"Required when status is blocked"}},"required":["status"]}`),
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

type toolResponse struct {
	Goal            *spec.SessionGoalRecord `json:"goal"`
	RemainingTokens *uint64                 `json:"remaining_tokens"`
}

func (s *Service) createTool(ctx context.Context, req tools.Request) (tools.Result, error) {
	var input struct {
		Objective   string  `json:"objective"`
		TokenBudget *uint64 `json:"token_budget"`
	}
	if err := decodeToolArgs(req, &input); err != nil {
		return tools.Result{}, err
	}
	workspaceID, sessionID := turnGoalIdentity(ctx, req.Addr.WorkspaceID, req.Addr.ThreadID)
	record, err := s.CreateGoal(ctx, workspaceID, sessionID, CreateInput{
		Objective: input.Objective, TokenBudget: input.TokenBudget,
	})
	if err != nil {
		return tools.Result{}, err
	}
	trackTurnGoal(ctx, record.ID)
	return goalToolResult(req, &record)
}

func (s *Service) getTool(ctx context.Context, req tools.Request) (tools.Result, error) {
	var input struct {
		ID string `json:"id"`
	}
	if err := decodeToolArgs(req, &input); err != nil {
		return tools.Result{}, err
	}
	workspaceID, sessionID := turnGoalIdentity(ctx, req.Addr.WorkspaceID, req.Addr.ThreadID)
	var record *spec.SessionGoalRecord
	if strings.TrimSpace(input.ID) == "" {
		current, err := s.CurrentGoal(ctx, workspaceID, sessionID)
		if err != nil {
			return tools.Result{}, err
		}
		record = current
	} else {
		found, err := s.Goal(ctx, workspaceID, sessionID, strings.TrimSpace(input.ID))
		if err != nil {
			return tools.Result{}, err
		}
		record = &found
	}
	return goalToolResult(req, record)
}

func (s *Service) updateTool(ctx context.Context, req tools.Request) (tools.Result, error) {
	var input struct {
		ID            string                 `json:"id"`
		Status        spec.SessionGoalStatus `json:"status"`
		Evidence      []string               `json:"evidence"`
		BlockedReason string                 `json:"blocked_reason"`
	}
	if err := decodeToolArgs(req, &input); err != nil {
		return tools.Result{}, err
	}
	workspaceID, sessionID := turnGoalIdentity(ctx, req.Addr.WorkspaceID, req.Addr.ThreadID)
	record, err := s.UpdateGoal(ctx, workspaceID, sessionID, UpdateInput{
		ID: input.ID, Status: input.Status, Evidence: input.Evidence, BlockedReason: input.BlockedReason,
	})
	if err != nil {
		return tools.Result{}, err
	}
	if record.Status == spec.SessionGoalActive {
		trackTurnGoal(ctx, record.ID)
	}
	return goalToolResult(req, &record)
}

func decodeToolArgs(req tools.Request, target any) error {
	if len(req.Arguments) == 0 {
		return nil
	}
	if err := json.Unmarshal(req.Arguments, target); err != nil {
		return errors.New("goal: invalid tool arguments")
	}
	return nil
}

func goalToolResult(req tools.Request, record *spec.SessionGoalRecord) (tools.Result, error) {
	response := toolResponse{Goal: record}
	if record != nil && record.TokenBudget != nil {
		remaining := uint64(0)
		if record.TokenUsage < *record.TokenBudget {
			remaining = *record.TokenBudget - record.TokenUsage
		}
		response.RemainingTokens = &remaining
	}
	payload, err := json.Marshal(response)
	if err != nil {
		return tools.Result{}, err
	}
	return tools.NewJSONResult(req.CallID, req.Name, payload)
}
