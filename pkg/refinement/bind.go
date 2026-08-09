package refinement

import "context"

type boundStore struct {
	base               Store
	logicalWorkspace   string
	backendWorkspaceID string
}

// BindStore maps the process-selected logical workspace to an authority's
// configured workspace while preserving explicitly addressed other workspaces.
func BindStore(base Store, logicalWorkspace, backendWorkspaceID string) Store {
	if base == nil || logicalWorkspace == "" || logicalWorkspace == backendWorkspaceID {
		return base
	}
	return &boundStore{base: base, logicalWorkspace: logicalWorkspace, backendWorkspaceID: backendWorkspaceID}
}

func (s *boundStore) Append(ctx context.Context, workspaceID, operationID string, request AppendRequest) (AppendResult, error) {
	return s.base.Append(ctx, s.backendWorkspace(workspaceID), operationID, request)
}

func (s *boundStore) Get(ctx context.Context, workspaceID, recordID string) (Record, error) {
	return s.base.Get(ctx, s.backendWorkspace(workspaceID), recordID)
}

func (s *boundStore) List(ctx context.Context, workspaceID string) ([]Record, error) {
	return s.base.List(ctx, s.backendWorkspace(workspaceID))
}

func (s *boundStore) backendWorkspace(workspaceID string) string {
	if workspaceID == "" || workspaceID == s.logicalWorkspace {
		return s.backendWorkspaceID
	}
	return workspaceID
}

type boundResourceEditor struct {
	base               ResourceEditor
	logicalWorkspace   string
	backendWorkspaceID string
}

func BindResourceEditor(base ResourceEditor, logicalWorkspace, backendWorkspaceID string) ResourceEditor {
	if base == nil || logicalWorkspace == "" || logicalWorkspace == backendWorkspaceID {
		return base
	}
	return &boundResourceEditor{base: base, logicalWorkspace: logicalWorkspace, backendWorkspaceID: backendWorkspaceID}
}

func (e *boundResourceEditor) Apply(ctx context.Context, workspaceID, operationID string, edit Edit) (Mutation, error) {
	if workspaceID == "" || workspaceID == e.logicalWorkspace {
		workspaceID = e.backendWorkspaceID
	}
	return e.base.Apply(ctx, workspaceID, operationID, edit)
}
