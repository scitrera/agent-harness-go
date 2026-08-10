package tui

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"charm.land/bubbles/v2/viewport"
	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/threadindex"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

type recordingDirectoryAccess struct {
	dirs []string
	err  error
}

type fixedExecutionBindingProvider struct {
	binding protocol.ExecutionBinding
}

func (p fixedExecutionBindingProvider) ExecutionBindingForDirectory(_ context.Context, _ string) (protocol.ExecutionBinding, error) {
	return p.binding, nil
}

func (r *recordingDirectoryAccess) GrantWorkingDirectory(dir string) error {
	r.dirs = append(r.dirs, dir)
	return r.err
}

func TestWorkingDirectoryCommandsStayInsideWorkspace(t *testing.T) {
	root := t.TempDir()
	subdirectory := filepath.Join(root, "src")
	if err := os.Mkdir(subdirectory, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	processCWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("get process cwd: %v", err)
	}
	m := model{
		workspaceRoot: root,
		cwd:           root,
		viewport:      viewport.New(),
		composer:      newComposer(),
		tailing:       true,
	}

	next, cmd := m.handleSlash("/cd src")
	if cmd != nil {
		t.Fatal("/cd should be local")
	}
	updated := next.(model)
	if updated.cwd != subdirectory {
		t.Fatalf("cwd = %q, want %q", updated.cwd, subdirectory)
	}
	if current, getErr := os.Getwd(); getErr != nil || current != processCWD {
		t.Fatalf("process cwd changed: got %q error %v, want %q", current, getErr, processCWD)
	}

	next, cmd = updated.handleSlash("/pwd")
	if cmd != nil {
		t.Fatal("/pwd should be local")
	}
	updated = next.(model)
	if got := updated.rows[len(updated.rows)-1].Text; got != subdirectory {
		t.Fatalf("/pwd output = %q, want %q", got, subdirectory)
	}

	next, _ = updated.handleSlash("/cd ..")
	updated = next.(model)
	if updated.cwd != root {
		t.Fatalf("cwd after .. = %q, want workspace root %q", updated.cwd, root)
	}
}

func TestWorkingDirectoryCommandAllowsExplicitExternalDirectory(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	access := &recordingDirectoryAccess{}
	file := filepath.Join(root, "file.txt")
	if err := os.WriteFile(file, []byte("content"), 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}
	m := model{
		workspaceRoot:   root,
		cwd:             root,
		directoryAccess: access,
		viewport:        viewport.New(),
		composer:        newComposer(),
		tailing:         true,
	}

	next, _ := m.handleSlash("/cd " + outside)
	updated := next.(model)
	if updated.cwd != outside {
		t.Fatalf("external /cd cwd = %q, want %q", updated.cwd, outside)
	}
	if len(access.dirs) != 1 || access.dirs[0] != outside {
		t.Fatalf("granted directories = %+v", access.dirs)
	}

	next, _ = m.handleSlash("/cd file.txt")
	updated = next.(model)
	if updated.cwd != root {
		t.Fatalf("file /cd changed cwd to %q", updated.cwd)
	}
	if got := updated.rows[len(updated.rows)-1].Text; !strings.Contains(got, "not a directory") {
		t.Fatalf("file /cd error = %q", got)
	}
}

func TestWorkingDirectoryAllowsExternalSymlinkTarget(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	access := &recordingDirectoryAccess{}
	link := filepath.Join(root, "outside-link")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	m := model{
		workspaceRoot:   root,
		cwd:             root,
		directoryAccess: access,
		viewport:        viewport.New(),
		composer:        newComposer(),
		tailing:         true,
	}

	next, _ := m.handleSlash("/cd outside-link")
	updated := next.(model)
	if updated.cwd != outside {
		t.Fatalf("symlink target cwd = %q, want %q", updated.cwd, outside)
	}
	if len(access.dirs) != 1 || access.dirs[0] != outside {
		t.Fatalf("symlink target grant = %+v", access.dirs)
	}
}

func TestWorkingDirectoryKeepsPriorCWDWhenExternalGrantFails(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	m := model{
		workspaceRoot:   root,
		cwd:             root,
		directoryAccess: &recordingDirectoryAccess{err: fmt.Errorf("denied")},
		viewport:        viewport.New(),
		composer:        newComposer(),
		tailing:         true,
	}

	next, _ := m.handleSlash("/cd " + outside)
	updated := next.(model)
	if updated.cwd != root {
		t.Fatalf("failed grant changed cwd to %q", updated.cwd)
	}
	if got := updated.rows[len(updated.rows)-1].Text; !strings.Contains(got, "denied") {
		t.Fatalf("grant failure = %q", got)
	}
}

func TestRemoteWorkingDirectoryUsesLogicalBindingWithoutAbsolutePathMetadata(t *testing.T) {
	root := t.TempDir()
	subdir := filepath.Join(root, "src")
	if err := os.Mkdir(subdir, 0o700); err != nil {
		t.Fatal(err)
	}
	index, err := threadindex.NewIndex(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := index.Touch("thread-1", ""); err != nil {
		t.Fatal(err)
	}
	binding := spec.NewExecutionBinding("project-a", "view-a", "us::drew::w1", spec.ExecutionSiteClient)
	binding.RootRef = "root:view-a"
	binding.RelativeDirectory = "src"
	channel := NewChannel()
	m := model{
		ctx: context.Background(), channel: channel, index: index,
		workspaceRoot: root, cwd: subdir, threadID: "thread-1",
		executionBindings: fixedExecutionBindingProvider{binding: binding},
		viewport:          viewport.New(), composer: newComposer(),
		turns: map[string]turnActivity{}, pendingApprovals: map[string]approvalRequest{},
		tools: map[string]toolEntry{}, tailing: true,
	}
	m.composer.SetValue("inspect the project")
	_, cmd := m.sendCurrent()
	if cmd == nil {
		t.Fatal("message was not enqueued")
	}
	if result := cmd(); result.(sendResultMsg).Err != nil {
		t.Fatal(result)
	}
	inbound, err := channel.FetchTask(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	got, err := spec.GetExecutionBinding(inbound.Message)
	if err != nil || got == nil || got.ViewID != "view-a" || got.RelativeDirectory != "src" {
		t.Fatalf("execution binding = %+v, %v", got, err)
	}
	if _, ok := tools.MessageWorkingDirectory(inbound.Message); ok {
		t.Fatal("remote message leaked absolute working_directory metadata")
	}
	text, _ := inbound.Message.Content[0].AsText()
	if strings.Contains(text.Text, root) {
		t.Fatalf("remote prompt leaked client root: %q", text.Text)
	}
}
