package sandbox

import (
	"context"
	"errors"
	"os/exec"
	"path/filepath"
	"testing"
)

func requirePython(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath(pythonBin); err != nil {
		t.Skipf("%s not on PATH", pythonBin)
	}
}

func newFileOps(t *testing.T) (*FileOps, string) {
	t.Helper()
	requirePython(t)
	return NewFileOps(NewLocalExecutor()), t.TempDir()
}

func TestExecuteBasics(t *testing.T) {
	requirePython(t)
	ex := NewLocalExecutor()
	if ex.ID() != "local" {
		t.Fatalf("ID = %q, want local", ex.ID())
	}
	res, err := ex.Execute(context.Background(), ExecRequest{Argv: []string{pythonBin, "-c", "import sys;print('hi');sys.exit(3)"}})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.ExitCode != 3 {
		t.Fatalf("ExitCode = %d, want 3", res.ExitCode)
	}
	if got := string(res.Stdout); got != "hi\n" {
		t.Fatalf("Stdout = %q, want %q", got, "hi\n")
	}
	if _, err := ex.Execute(context.Background(), ExecRequest{}); !errors.Is(err, ErrEmptyArgv) {
		t.Fatalf("empty argv err = %v, want ErrEmptyArgv", err)
	}
}

func TestWriteReadRoundTrip(t *testing.T) {
	fo, dir := newFileOps(t)
	ctx := context.Background()
	p := filepath.Join(dir, "sub", "hello.txt")
	const content = "alpha\nbeta\ngamma\n"
	if err := fo.Write(ctx, p, content); err != nil {
		t.Fatalf("Write: %v", err)
	}
	got, err := fo.Read(ctx, p, 0, 0)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if got != content {
		t.Fatalf("Read = %q, want %q", got, content)
	}
}

func TestReadOffsetLimit(t *testing.T) {
	fo, dir := newFileOps(t)
	ctx := context.Background()
	p := filepath.Join(dir, "lines.txt")
	if err := fo.Write(ctx, p, "l0\nl1\nl2\nl3\nl4\n"); err != nil {
		t.Fatalf("Write: %v", err)
	}
	got, err := fo.Read(ctx, p, 1, 2)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if got != "l1\nl2\n" {
		t.Fatalf("Read offset/limit = %q, want %q", got, "l1\nl2\n")
	}
}

func TestEditSuccessAndErrors(t *testing.T) {
	fo, dir := newFileOps(t)
	ctx := context.Background()
	p := filepath.Join(dir, "edit.txt")
	if err := fo.Write(ctx, p, "one two three\n"); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := fo.Edit(ctx, p, "two", "TWO"); err != nil {
		t.Fatalf("Edit: %v", err)
	}
	got, _ := fo.Read(ctx, p, 0, 0)
	if got != "one TWO three\n" {
		t.Fatalf("after Edit = %q", got)
	}
	if err := fo.Edit(ctx, p, "missing", "x"); !errors.Is(err, ErrOldTextNotFound) {
		t.Fatalf("Edit missing err = %v, want ErrOldTextNotFound", err)
	}
	if err := fo.Write(ctx, p, "dup dup\n"); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := fo.Edit(ctx, p, "dup", "x"); !errors.Is(err, ErrOldTextAmbiguous) {
		t.Fatalf("Edit ambiguous err = %v, want ErrOldTextAmbiguous", err)
	}
}

func TestLs(t *testing.T) {
	fo, dir := newFileOps(t)
	ctx := context.Background()
	if err := fo.Write(ctx, filepath.Join(dir, "a.txt"), "a"); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := fo.Write(ctx, filepath.Join(dir, "d", "b.txt"), "b"); err != nil {
		t.Fatalf("Write: %v", err)
	}
	entries, err := fo.Ls(ctx, dir)
	if err != nil {
		t.Fatalf("Ls: %v", err)
	}
	byName := map[string]DirEntry{}
	for _, e := range entries {
		byName[e.Name] = e
	}
	if e, ok := byName["a.txt"]; !ok || e.IsDir || e.Size != 1 {
		t.Fatalf("a.txt entry = %+v ok=%v", e, ok)
	}
	if e, ok := byName["d"]; !ok || !e.IsDir {
		t.Fatalf("d entry = %+v ok=%v", e, ok)
	}
}

func TestGlob(t *testing.T) {
	fo, dir := newFileOps(t)
	ctx := context.Background()
	for _, name := range []string{"x.go", "y.go", "z.txt"} {
		if err := fo.Write(ctx, filepath.Join(dir, name), "x"); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	paths, err := fo.Glob(ctx, filepath.Join(dir, "*.go"))
	if err != nil {
		t.Fatalf("Glob: %v", err)
	}
	if len(paths) != 2 {
		t.Fatalf("Glob got %d paths, want 2: %v", len(paths), paths)
	}
}

func TestGrep(t *testing.T) {
	fo, dir := newFileOps(t)
	ctx := context.Background()
	p := filepath.Join(dir, "src.txt")
	if err := fo.Write(ctx, p, "needle here\nhaystack\nanother needle\n"); err != nil {
		t.Fatalf("Write: %v", err)
	}
	matches, err := fo.Grep(ctx, "needle", dir)
	if err != nil {
		t.Fatalf("Grep: %v", err)
	}
	if len(matches) != 2 {
		t.Fatalf("Grep got %d matches, want 2: %+v", len(matches), matches)
	}
	for _, m := range matches {
		if m.LineNumber == 0 || m.Line == "" || m.Path == "" {
			t.Fatalf("incomplete match: %+v", m)
		}
	}
}

func TestDelete(t *testing.T) {
	fo, dir := newFileOps(t)
	ctx := context.Background()
	p := filepath.Join(dir, "gone.txt")
	if err := fo.Write(ctx, p, "bye"); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := fo.Delete(ctx, p); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := fo.Read(ctx, p, 0, 0); !errors.Is(err, ErrOpFailed) {
		t.Fatalf("Read after Delete err = %v, want ErrOpFailed", err)
	}
}

func TestUploadDownload(t *testing.T) {
	requirePython(t)
	ex := NewLocalExecutor()
	ctx := context.Background()
	p := filepath.Join(t.TempDir(), "raw.bin")
	data := []byte{0, 1, 2, 3, 255}
	if err := ex.Upload(ctx, p, data); err != nil {
		t.Fatalf("Upload: %v", err)
	}
	got, err := ex.Download(ctx, p)
	if err != nil {
		t.Fatalf("Download: %v", err)
	}
	if string(got) != string(data) {
		t.Fatalf("Download = %v, want %v", got, data)
	}
}
