package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"
)

// todoWriteArgs is the model-facing argument shape for the todo_write tool.
type todoWriteArgs struct {
	ID    string `json:"id"`
	Title string `json:"title"`
	Items []struct {
		ID         string `json:"id"`
		Content    string `json:"content"`
		Status     string `json:"status"`
		ActiveForm string `json:"active_form"`
		// ActiveFormAlt accepts the no-underscore "activeform" some models emit
		// instead of the schema's "active_form"; used only when active_form is absent.
		ActiveFormAlt string `json:"activeform"`
	} `json:"items"`
}

// todoWrite is the built-in shared-checklist tool. The model passes the full
// current list; the tool surfaces it as a spec `todo` content part on the
// message stream (appended once, then patched in place on later writes via the
// PartEmitter) so the frontend and the agent share one live, persisted board.
func todoWrite(ctx context.Context, req Request) (Result, error) {
	var args todoWriteArgs
	if len(req.Arguments) > 0 {
		if err := json.Unmarshal(req.Arguments, &args); err != nil {
			return Result{}, fmt.Errorf("%w: todo_write args: %v", ErrInvalidArgument, err)
		}
	}
	id := args.ID
	if id == "" {
		id = "todo_main"
	}
	items := make([]spec.TodoItem, 0, len(args.Items))
	for i, it := range args.Items {
		status := spec.TodoStatus(it.Status)
		if status == "" {
			status = spec.TodoPending
		}
		itemID := it.ID
		if itemID == "" {
			// Stable positional id for id-less items. The model rewrites the FULL
			// board each call, so <board>#<index> keeps a re-worded item at the same
			// position mapped to the SAME durable world-state entry (updated in
			// place). Without it the sink falls back to keying on content, so every
			// re-wording spawned a NEW entry and stale todos piled up in the
			// "## Todos" prompt section over a long task.
			itemID = fmt.Sprintf("%s#%d", id, i)
		}
		activeForm := it.ActiveForm
		if activeForm == "" {
			activeForm = it.ActiveFormAlt // tolerate the "activeform" alias
		}
		items = append(items, spec.TodoItem{
			ID:         itemID,
			Content:    it.Content,
			Status:     status,
			ActiveForm: activeForm,
		})
	}
	part := spec.NewTodoPart(spec.TodoPart{ID: id, Title: args.Title, Items: items})

	if emitter, ok := PartEmitterFrom(ctx); ok {
		if err := emitter.UpsertPart(ctx, part); err != nil {
			return Result{}, fmt.Errorf("emit todo part: %w", err)
		}
	}
	// Record the board into the durable, aged world-state so the todo list
	// survives compaction (the emitted part is folded into the assistant message,
	// which is droppable; this ledger rides ExtractWorldState).
	if sink, ok := WorldStateSinkFrom(ctx); ok {
		sink.RecordTodos(items)
	}

	// Echo a concise summary back as the tool result so the model sees the
	// recorded state (counts by status), without re-sending the whole list.
	payload, err := json.Marshal(map[string]any{
		"todo_id":   id,
		"count":     len(items),
		"by_status": statusCounts(items),
	})
	if err != nil {
		return Result{}, fmt.Errorf("encode todo result: %w", err)
	}
	return NewJSONResult(req.CallID, req.Name, payload)
}

func statusCounts(items []spec.TodoItem) map[string]int {
	counts := map[string]int{}
	for _, it := range items {
		counts[string(it.Status)]++
	}
	return counts
}

// todoDescriptor is the model-facing descriptor for the todo_write tool.
func todoDescriptor() Descriptor {
	keys := []string{string(spec.TodoPending), string(spec.TodoInProgress), string(spec.TodoCompleted), string(spec.TodoCancelled)}
	sort.Strings(keys)
	statusEnum, _ := json.Marshal(keys)
	params := fmt.Sprintf(`{"type":"object","properties":{`+
		`"id":{"type":"string","description":"Stable list id; omit to use the default board"},`+
		`"title":{"type":"string","description":"Optional list title"},`+
		`"items":{"type":"array","description":"The FULL current list (send every item each call, not a delta)","items":{"type":"object","properties":{`+
		`"id":{"type":"string"},`+
		`"content":{"type":"string","description":"Imperative task description"},`+
		`"status":{"type":"string","enum":%s},`+
		`"active_form":{"type":"string","description":"Present-tense label shown while in_progress"}`+
		`},"required":["content","status"]}}},"required":["items"]}`, string(statusEnum))
	return Descriptor{
		Name:        "todo_write",
		Description: "Write the shared task checklist (todo list) the user sees. Pass the FULL current list each call; use it to plan multi-step work and keep exactly one item in_progress.",
		Parameters:  json.RawMessage(params),
		// The board is UI state, not the user's data: it mutates only what the
		// user is already watching, so it reads as an interaction rather than a
		// write worth stopping for.
		Effect: spec.ToolEffectInteraction,
	}
}
