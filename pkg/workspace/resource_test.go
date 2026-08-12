package workspace

import (
	"testing"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"
)

func TestExecutionViewResourceIDIsExactAndPathSafe(t *testing.T) {
	binding := spec.NewExecutionBinding("acme docs", "worktree/feature", "us::alice::window-1", spec.ExecutionSiteClient)
	binding.RootRef = "private/root"
	got, err := ExecutionViewResourceID(binding)
	if err != nil {
		t.Fatal(err)
	}
	want := "workspaces/acme%20docs/views/worktree%2Ffeature/hosts/us%3A%3Aalice%3A%3Awindow-1"
	if got != want {
		t.Fatalf("resource = %q, want %q", got, want)
	}
}

func TestExecutionViewRequiredAccessUsesPolicyCeiling(t *testing.T) {
	read, err := ExecutionViewRequiredAccess(ExecutionViewPolicy{WriteAccess: ViewWriteAccessReadOnly})
	if err != nil || read != ExecutionViewReadAccess {
		t.Fatalf("read access = %d, %v", read, err)
	}
	write, err := ExecutionViewRequiredAccess(ExecutionViewPolicy{WriteAccess: ViewWriteAccessReadWrite})
	if err != nil || write != ExecutionViewWriteAccess {
		t.Fatalf("write access = %d, %v", write, err)
	}
}
