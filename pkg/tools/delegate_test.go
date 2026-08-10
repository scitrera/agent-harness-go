package tools

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/localtools"
)

// fakeFileDelegate records calls and returns canned content, so a test can prove
// the file handlers route through the ctx delegate instead of the workspace.
type fakeFileDelegate struct {
	readPath             string
	content              string
	wrotePath, wroteBody string
	editPath, editOld    string
	editNew              string
}

func (f *fakeFileDelegate) ReadFile(_ context.Context, path string, _ int64) (string, error) {
	f.readPath = path
	return f.content, nil
}

func (f *fakeFileDelegate) WriteFile(_ context.Context, path, content string) error {
	f.wrotePath, f.wroteBody = path, content
	return nil
}

func (f *fakeFileDelegate) EditFile(_ context.Context, path, oldText, newText string) error {
	f.editPath, f.editOld, f.editNew = path, oldText, newText
	return nil
}

type fakeCommandDelegate struct {
	spec localtools.CommandSpec
	res  localtools.CommandResult
}

type fakeToolDelegate struct{}

func (*fakeToolDelegate) HandlesTool(string) bool { return true }

func (*fakeToolDelegate) InvokeTool(context.Context, Request) (Result, error) {
	return Result{}, nil
}

func (f *fakeCommandDelegate) RunCommand(_ context.Context, spec localtools.CommandSpec) (localtools.CommandResult, error) {
	f.spec = spec
	return f.res, nil
}

func delegateTestRegistry(t *testing.T) (*Registry, *localtools.Workspace) {
	t.Helper()
	ws, err := localtools.NewWorkspace(t.TempDir())
	if err != nil {
		t.Fatalf("workspace: %v", err)
	}
	reg := NewRegistry()
	if err := RegisterLocal(reg, LocalConfig{Workspace: ws, Python: "python3"}); err != nil {
		t.Fatalf("register: %v", err)
	}
	return reg, ws
}

// TestFileDelegateOverridesReadWriteEdit asserts a ctx FileDelegate intercepts
// read_file/write_file/edit_file instead of the local workspace.
func TestFileDelegateOverridesReadWriteEdit(t *testing.T) {
	reg, _ := delegateTestRegistry(t)
	fake := &fakeFileDelegate{content: "from-delegate"}
	ctx := WithFileDelegate(context.Background(), fake)

	res, err := reg.Invoke(ctx, Request{CallID: "c1", Name: "read_file", Arguments: json.RawMessage(`{"path":"a.txt"}`)})
	if err != nil {
		t.Fatalf("read invoke: %v", err)
	}
	var rd struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(res.Payload, &rd); err != nil {
		t.Fatalf("read payload: %v", err)
	}
	if rd.Text != "from-delegate" || fake.readPath != "a.txt" {
		t.Fatalf("read = %q via %q, want delegate content", rd.Text, fake.readPath)
	}

	wres, err := reg.Invoke(ctx, Request{CallID: "c2", Name: "write_file", Arguments: json.RawMessage(`{"path":"b.txt","content":"hello"}`)})
	if err != nil {
		t.Fatalf("write invoke: %v", err)
	}
	if fake.wrotePath != "b.txt" || fake.wroteBody != "hello" {
		t.Fatalf("write delegate not called: %+v", fake)
	}
	if len(wres.Metadata.FileChanges) != 1 || wres.Metadata.FileChanges[0].Kind != "write" {
		t.Fatalf("write FileChanges not preserved: %+v", wres.Metadata.FileChanges)
	}

	if _, err := reg.Invoke(ctx, Request{CallID: "c3", Name: "edit_file", Arguments: json.RawMessage(`{"path":"c.txt","old_text":"x","new_text":"y"}`)}); err != nil {
		t.Fatalf("edit invoke: %v", err)
	}
	if fake.editPath != "c.txt" || fake.editOld != "x" || fake.editNew != "y" {
		t.Fatalf("edit delegate not called: %+v", fake)
	}
}

func TestWithoutToolDelegateMasksOuterDelegate(t *testing.T) {
	delegate := &fakeToolDelegate{}
	ctx := WithToolDelegate(context.Background(), delegate)
	if ToolDelegateFrom(ctx) == nil {
		t.Fatal("outer delegate missing")
	}
	if ToolDelegateFrom(WithoutToolDelegate(ctx)) != nil {
		t.Fatal("host-local boundary retained outer delegate")
	}
}

// TestFileDelegateAbsentFallsBackToWorkspace asserts that with no delegate on
// ctx the handlers read/write the local workspace.
func TestFileDelegateAbsentFallsBackToWorkspace(t *testing.T) {
	reg, ws := delegateTestRegistry(t)
	ctx := context.Background()
	if err := ws.WriteFile(ctx, "local.txt", "on-disk"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	res, err := reg.Invoke(ctx, Request{CallID: "c1", Name: "read_file", Arguments: json.RawMessage(`{"path":"local.txt"}`)})
	if err != nil {
		t.Fatalf("read invoke: %v", err)
	}
	var rd struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(res.Payload, &rd); err != nil {
		t.Fatalf("read payload: %v", err)
	}
	if rd.Text != "on-disk" {
		t.Fatalf("read = %q, want workspace content 'on-disk'", rd.Text)
	}
}

// TestCommandDelegateOverridesShell asserts a ctx CommandDelegate intercepts the
// shell tool instead of the local workspace.
func TestCommandDelegateOverridesShell(t *testing.T) {
	reg, _ := delegateTestRegistry(t)
	fake := &fakeCommandDelegate{res: localtools.CommandResult{ExitCode: 0, Output: "delegated-output"}}
	ctx := WithCommandDelegate(context.Background(), fake)

	res, err := reg.Invoke(ctx, Request{CallID: "c1", Name: "shell", Arguments: json.RawMessage(`{"command":"echo","args":["hi"]}`)})
	if err != nil {
		t.Fatalf("shell invoke: %v", err)
	}
	if fake.spec.Name != "echo" || len(fake.spec.Args) != 1 || fake.spec.Args[0] != "hi" {
		t.Fatalf("command delegate spec = %+v, want echo hi", fake.spec)
	}
	var out struct {
		Output string `json:"output"`
	}
	if err := json.Unmarshal(res.Payload, &out); err != nil {
		t.Fatalf("shell payload: %v", err)
	}
	if out.Output != "delegated-output" {
		t.Fatalf("shell output = %q, want delegated-output", out.Output)
	}
}

func TestShellCommandLineUsesInterpreterWhenArgsAreOmitted(t *testing.T) {
	reg, _ := delegateTestRegistry(t)
	fake := &fakeCommandDelegate{res: localtools.CommandResult{ExitCode: 0, Output: "interpreted"}}
	ctx := WithCommandDelegate(context.Background(), fake)

	_, err := reg.Invoke(ctx, Request{
		CallID:    "c1",
		Name:      "shell",
		Arguments: json.RawMessage(`{"command":"printf hello | tr a-z A-Z","cwd":"/tmp"}`),
	})
	if err != nil {
		t.Fatalf("shell command-line invoke: %v", err)
	}
	if fake.spec.Name != "/bin/sh" || len(fake.spec.Args) != 2 || fake.spec.Args[0] != "-lc" || fake.spec.Args[1] != "printf hello | tr a-z A-Z" {
		t.Fatalf("interpreted command spec = %+v", fake.spec)
	}
	if fake.spec.CWD != "/tmp" {
		t.Fatalf("interpreted command cwd = %q", fake.spec.CWD)
	}
}
