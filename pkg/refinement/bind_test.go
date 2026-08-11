package refinement

import (
	"context"
	"testing"
)

func TestWorkspaceBindingsMapOnlySelectedLogicalWorkspace(t *testing.T) {
	store := &capturingStore{}
	boundStore := BindStore(store, "logical", "backend")
	_, _ = boundStore.Query(context.Background(), "logical", Query{})
	_, _ = boundStore.Query(context.Background(), "other", Query{})
	if len(store.workspaces) != 2 || store.workspaces[0] != "backend" || store.workspaces[1] != "other" {
		t.Fatalf("store workspaces = %v", store.workspaces)
	}

	editor := &capturingEditor{}
	boundEditor := BindResourceEditor(editor, "logical", "backend")
	_, _ = boundEditor.Apply(context.Background(), "logical", "op-1", Edit{})
	_, _ = boundEditor.Apply(context.Background(), "other", "op-2", Edit{})
	if len(editor.workspaces) != 2 || editor.workspaces[0] != "backend" || editor.workspaces[1] != "other" {
		t.Fatalf("editor workspaces = %v", editor.workspaces)
	}
}

type capturingStore struct{ workspaces []string }

func (s *capturingStore) Append(_ context.Context, workspaceID, _ string, _ AppendRequest) (AppendResult, error) {
	s.workspaces = append(s.workspaces, workspaceID)
	return AppendResult{}, nil
}

func (s *capturingStore) Get(_ context.Context, workspaceID, _ string) (Record, error) {
	s.workspaces = append(s.workspaces, workspaceID)
	return Record{}, nil
}

func (s *capturingStore) Query(_ context.Context, workspaceID string, _ Query) (Page, error) {
	s.workspaces = append(s.workspaces, workspaceID)
	return Page{}, nil
}

type capturingEditor struct{ workspaces []string }

func (e *capturingEditor) Apply(_ context.Context, workspaceID, _ string, _ Edit) (Mutation, error) {
	e.workspaces = append(e.workspaces, workspaceID)
	return Mutation{}, nil
}
