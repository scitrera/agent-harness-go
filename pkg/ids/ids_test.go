package ids

import (
	"strings"
	"testing"
)

func Test_New_prefixes_and_is_unique(t *testing.T) {
	a, err := New("th-")
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if !strings.HasPrefix(a, "th-") {
		t.Fatalf("missing prefix: %q", a)
	}
	if len(a) != len("th-")+16 { // 8 random bytes → 16 hex chars
		t.Fatalf("unexpected id length: %q", a)
	}
	if b, _ := New("th-"); a == b {
		t.Fatalf("ids should be unique, got %q twice", a)
	}
}
