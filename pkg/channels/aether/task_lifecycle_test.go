package aether

import (
	"testing"

	pb "github.com/scitrera/aether/api/proto"
	sdk "github.com/scitrera/aether/sdk/go/aether"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

func TestTaskInfoAuthorityCanonicalizesAetherPrincipalType(t *testing.T) {
	got, err := TaskInfoAuthority(&sdk.TaskInfo{
		AuthorityMode: "on_behalf_of", AuthorityGrantID: "grant-task",
		SubjectType: "User", SubjectID: "alice",
	})
	if err != nil {
		t.Fatalf("TaskInfoAuthority() error = %v", err)
	}
	want := tools.MemoryAuthority{GrantID: "grant-task", SubjectType: "user", SubjectID: "alice"}
	if got != want {
		t.Fatalf("TaskInfoAuthority() = %+v, want %+v", got, want)
	}
}

func TestAssignedAuthorityCanonicalizesAetherPrincipalType(t *testing.T) {
	grantID, subjectType, subjectID, err := assignedAuthority(&pb.AuthorizationContext{
		AuthorityMode: "on_behalf_of", GrantId: "grant-task",
		Subject: &pb.PrincipalRef{PrincipalType: "User", PrincipalId: "alice"},
	})
	if err != nil {
		t.Fatalf("assignedAuthority() error = %v", err)
	}
	if grantID != "grant-task" || subjectType != "user" || subjectID != "alice" {
		t.Fatalf("assignedAuthority() = (%q, %q, %q)", grantID, subjectType, subjectID)
	}
}
