package subagent

import (
	"context"
	"errors"
	"testing"
)

type definitionProviderStub struct {
	workspaces []string
	defs       []Definition
	err        error
}

func (p *definitionProviderStub) LoadWorkspace(_ context.Context, workspaceID string) ([]Definition, error) {
	p.workspaces = append(p.workspaces, workspaceID)
	return p.defs, p.err
}

func TestProviderCatalogBindsDefaultAndPassesExplicitWorkspaces(t *testing.T) {
	provider := &definitionProviderStub{defs: []Definition{{
		Name: "reviewer", Type: "reviewer", Description: "Review evidence", Prompt: "Cite evidence",
	}}}
	catalog, err := NewProviderCatalog(provider, "project", "ml-project")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.Get(context.Background(), "reviewer"); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.GetWorkspace(context.Background(), "other", "reviewer"); err != nil {
		t.Fatal(err)
	}
	if len(provider.workspaces) != 2 || provider.workspaces[0] != "ml-project" || provider.workspaces[1] != "other" {
		t.Fatalf("workspaces = %#v", provider.workspaces)
	}
}

func TestProviderCatalogFailsClosedAndRejectsDuplicates(t *testing.T) {
	loadErr := errors.New("authority unavailable")
	catalog, _ := NewProviderCatalog(&definitionProviderStub{err: loadErr}, "project", "project")
	if _, err := catalog.List(context.Background()); !errors.Is(err, loadErr) {
		t.Fatalf("load error = %v", err)
	}

	duplicate := Definition{Name: "reviewer", Type: "reviewer", Description: "Review", Prompt: "Review"}
	catalog, _ = NewProviderCatalog(&definitionProviderStub{defs: []Definition{duplicate, duplicate}}, "project", "project")
	if _, err := catalog.List(context.Background()); !errors.Is(err, ErrInvalidDefinition) {
		t.Fatalf("duplicate error = %v", err)
	}
}
