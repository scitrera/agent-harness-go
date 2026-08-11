package main

import (
	"context"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/harness"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/store"
	"github.com/scitrera/agent-harness-go/pkg/threadindex"
	"github.com/scitrera/agent-harness-go/pkg/tools"
	"github.com/scitrera/agent-harness-go/pkg/turn"
)

func TestBoundHistoryScopesLegacyUISurface(t *testing.T) {
	ctx := context.Background()
	files := store.NewFileStore("", t.TempDir())
	bound := bindHistory(files, "project-a")
	messageA := []protocol.ChatMessage{{ID: "a", Addr: protocol.MessageAddress{WorkspaceID: "project-a", ThreadID: "shared"}}}
	messageB := []protocol.ChatMessage{{ID: "b", Addr: protocol.MessageAddress{WorkspaceID: "project-b", ThreadID: "shared"}}}

	if err := bound.SaveHistory(ctx, "shared", messageA); err != nil {
		t.Fatalf("save bound: %v", err)
	}
	scoped := bound.(harness.WorkspaceHistoryStore)
	if err := scoped.SaveWorkspaceHistory(ctx, "project-b", "shared", messageB); err != nil {
		t.Fatalf("save explicit workspace: %v", err)
	}
	got, err := bound.LoadHistory(ctx, "shared")
	if err != nil || len(got) != 1 || got[0].ID != "a" {
		t.Fatalf("bound load: %v %+v", err, got)
	}
	legacy, err := files.LoadHistory(ctx, "shared")
	if err != nil || len(legacy) != 0 {
		t.Fatalf("legacy history leaked: %v %+v", err, legacy)
	}
	if err := bound.DeleteHistory(ctx, "shared"); err != nil {
		t.Fatalf("delete bound: %v", err)
	}
	other, err := files.LoadWorkspaceHistory(ctx, "project-b", "shared")
	if err != nil || len(other) != 1 || other[0].ID != "b" {
		t.Fatalf("other workspace changed: %v %+v", err, other)
	}
}

type legacyHistoryStore struct{}

func (legacyHistoryStore) LoadHistory(context.Context, string) ([]protocol.ChatMessage, error) {
	return nil, nil
}
func (legacyHistoryStore) SaveHistory(context.Context, string, []protocol.ChatMessage) error {
	return nil
}
func (legacyHistoryStore) DeleteHistory(context.Context, string) error { return nil }

func TestBoundLegacyBackendRejectsDifferentExplicitWorkspace(t *testing.T) {
	bound := bindHistory(legacyHistoryStore{}, "project-a")
	scoped := bound.(harness.WorkspaceHistoryStore)
	if _, err := scoped.LoadWorkspaceHistory(context.Background(), "project-b", "shared"); err == nil {
		t.Fatal("expected cross-workspace load to fail for a single-workspace backend")
	}
}

func TestBoundLegacyBackendAcceptsExplicitBackendWorkspaceOverride(t *testing.T) {
	bound := bindHistoryBackend(legacyHistoryStore{}, "project-a", "memorylayer-a")
	scoped := bound.(harness.WorkspaceHistoryStore)
	if _, err := scoped.LoadWorkspaceHistory(context.Background(), "memorylayer-a", "shared"); err != nil {
		t.Fatalf("load explicit backend workspace: %v", err)
	}
}

type recordingScopedHistory struct {
	legacyHistoryStore
	workspaceID string
}

func (s *recordingScopedHistory) LoadWorkspaceHistory(_ context.Context, workspaceID, _ string) ([]protocol.ChatMessage, error) {
	s.workspaceID = workspaceID
	return nil, nil
}

func (s *recordingScopedHistory) SaveWorkspaceHistory(_ context.Context, workspaceID, _ string, _ []protocol.ChatMessage) error {
	s.workspaceID = workspaceID
	return nil
}

func (s *recordingScopedHistory) DeleteWorkspaceHistory(_ context.Context, workspaceID, _ string) error {
	s.workspaceID = workspaceID
	return nil
}

func TestBoundScopedBackendMapsLogicalWorkspaceToOverride(t *testing.T) {
	base := &recordingScopedHistory{}
	bound := bindHistoryBackend(base, "project-a", "memorylayer-a")
	scoped := bound.(harness.WorkspaceHistoryStore)
	if _, err := scoped.LoadWorkspaceHistory(context.Background(), "project-a", "shared"); err != nil {
		t.Fatal(err)
	}
	if base.workspaceID != "memorylayer-a" {
		t.Fatalf("backend workspace = %q", base.workspaceID)
	}
	if err := scoped.SaveWorkspaceHistory(context.Background(), "project-b", "shared", nil); err != nil {
		t.Fatal(err)
	}
	if base.workspaceID != "project-b" {
		t.Fatalf("dynamic backend workspace = %q", base.workspaceID)
	}
}

type recordingWorkspaceThreads struct {
	workspaceID string
	lookupID    string
}

func (s *recordingWorkspaceThreads) List() []threadindex.Session { return nil }
func (s *recordingWorkspaceThreads) Create() (threadindex.Session, error) {
	return threadindex.Session{}, nil
}
func (s *recordingWorkspaceThreads) Touch(string, string) error  { return nil }
func (s *recordingWorkspaceThreads) Rename(string, string) error { return nil }
func (s *recordingWorkspaceThreads) Delete(string) error         { return nil }
func (s *recordingWorkspaceThreads) RefreshWorkspace(_ context.Context, workspaceID string) error {
	s.workspaceID = workspaceID
	return nil
}
func (s *recordingWorkspaceThreads) ListWorkspace(workspaceID string) []threadindex.Session {
	s.workspaceID = workspaceID
	return nil
}
func (s *recordingWorkspaceThreads) CreateWorkspaceThread(workspaceID string) (threadindex.Session, error) {
	s.workspaceID = workspaceID
	return threadindex.Session{}, nil
}
func (s *recordingWorkspaceThreads) TouchWorkspaceThread(workspaceID, _, _ string) error {
	s.workspaceID = workspaceID
	return nil
}
func (s *recordingWorkspaceThreads) RenameWorkspaceThread(workspaceID, _, _ string) error {
	s.workspaceID = workspaceID
	return nil
}
func (s *recordingWorkspaceThreads) DeleteWorkspaceThread(workspaceID, _ string) error {
	s.workspaceID = workspaceID
	return nil
}
func (s *recordingWorkspaceThreads) LookupWorkspaceThread(_ context.Context, workspaceID, id string) (threadindex.Session, bool, error) {
	s.workspaceID = workspaceID
	s.lookupID = id
	return threadindex.Session{ID: id}, true, nil
}

func TestBoundThreadsMapsOnlySelectedLogicalWorkspace(t *testing.T) {
	base := &recordingWorkspaceThreads{}
	bound := bindThreads(base, "project-a", "memorylayer-a")
	if err := bound.TouchWorkspaceThread("project-a", "thread", "hello"); err != nil {
		t.Fatal(err)
	}
	if base.workspaceID != "memorylayer-a" {
		t.Fatalf("selected backend workspace = %q", base.workspaceID)
	}
	if err := bound.TouchWorkspaceThread("project-b", "thread", "hello"); err != nil {
		t.Fatal(err)
	}
	if base.workspaceID != "project-b" {
		t.Fatalf("dynamic backend workspace = %q", base.workspaceID)
	}
	lookup, ok := bound.(threadindex.WorkspaceLookup)
	if !ok {
		t.Fatal("bound threads lost authoritative lookup capability")
	}
	if session, found, err := lookup.LookupWorkspaceThread(context.Background(), "project-a", "scheduled-thread"); err != nil || !found || session.ID != "scheduled-thread" {
		t.Fatalf("lookup session=%+v found=%v err=%v", session, found, err)
	}
	if base.workspaceID != "memorylayer-a" || base.lookupID != "scheduled-thread" {
		t.Fatalf("lookup backend workspace=%q thread=%q", base.workspaceID, base.lookupID)
	}
}

type recordingMemoryService struct{ workspaceID string }

func (s *recordingMemoryService) Recall(_ context.Context, _ tools.MemoryAuthority, workspaceID, _ string, _ int) ([]tools.MemoryHit, error) {
	s.workspaceID = workspaceID
	return nil, nil
}

func (s *recordingMemoryService) AppendThreadMessages(_ context.Context, _ tools.MemoryAuthority, workspaceID, _, _ string, _ []protocol.ChatMessage) error {
	s.workspaceID = workspaceID
	return nil
}

func TestBoundMemoryMapsLogicalWorkspaceToExplicitBackendOverride(t *testing.T) {
	base := &recordingMemoryService{}
	memory := bindMemory(base, "project-a", "memorylayer-a")
	if _, err := memory.Recall(context.Background(), tools.MemoryAuthority{}, "project-a", "query", 5); err != nil {
		t.Fatalf("recall: %v", err)
	}
	if base.workspaceID != "memorylayer-a" {
		t.Fatalf("backend workspace = %q", base.workspaceID)
	}
	if _, err := memory.Recall(context.Background(), tools.MemoryAuthority{}, "project-b", "query", 5); err != nil {
		t.Fatalf("dynamic recall: %v", err)
	}
	if base.workspaceID != "project-b" {
		t.Fatalf("dynamic backend workspace = %q", base.workspaceID)
	}
}

type recordingMemoryRegistrar struct {
	recordingMemoryService
	spec turn.ThreadSpec
}

func (s *recordingMemoryRegistrar) EnsureThread(_ context.Context, _ tools.MemoryAuthority, spec turn.ThreadSpec) (string, error) {
	s.spec = spec
	return "thread-server", nil
}

func TestBoundMemoryPreservesRegistrarAndMapsItsWorkspace(t *testing.T) {
	base := &recordingMemoryRegistrar{}
	memory := bindMemory(base, "project-a", "memorylayer-a")
	registrar, ok := memory.(turn.ThreadRegistrar)
	if !ok {
		t.Fatal("bound registrar capability was lost")
	}
	id, err := registrar.EnsureThread(context.Background(), tools.MemoryAuthority{}, turn.ThreadSpec{
		WorkspaceID: "project-a", ParentThreadID: "parent-1", Origin: "subagent",
	})
	if err != nil {
		t.Fatal(err)
	}
	if id != "thread-server" || base.spec.WorkspaceID != "memorylayer-a" || base.spec.ParentThreadID != "parent-1" {
		t.Fatalf("id=%q spec=%#v", id, base.spec)
	}
}

func TestBoundMemoryDoesNotAdvertiseRegistrarWhenBaseLacksIt(t *testing.T) {
	memory := bindMemory(&recordingMemoryService{}, "project-a", "memorylayer-a")
	if _, ok := memory.(turn.ThreadRegistrar); ok {
		t.Fatal("bound non-registrar falsely advertised ThreadRegistrar")
	}
}
