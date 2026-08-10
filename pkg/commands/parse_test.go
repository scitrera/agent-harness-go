package commands

import "testing"

func TestParse(t *testing.T) {
	cases := []struct {
		line     string
		wantName string
		wantArgs string
		wantOK   bool
	}{
		{"/help", "help", "", true},
		{"  /help  ", "help", "", true},
		{"/commit fix the bug", "commit", "fix the bug", true},
		{"/think: high", "think", "high", true},
		{"/think:high", "think:high", "", true}, // ':' only stripped when trailing on the name token
		{"/git:commit scope", "git:commit", "scope", true},
		{"/DOCK-Discord  ON", "dock-discord", "ON", true},
		{"hello world", "", "", false},
		{"", "", "", false},
		{"/", "", "", false},
		{"//comment", "", "", false},
		{"/ leading space", "", "", false},
		{"/commit\tfix tab", "commit", "fix tab", true},
	}
	for _, c := range cases {
		name, args, ok := Parse(c.line)
		if ok != c.wantOK || name != c.wantName || args != c.wantArgs {
			t.Errorf("Parse(%q) = (%q, %q, %v), want (%q, %q, %v)", c.line, name, args, ok, c.wantName, c.wantArgs, c.wantOK)
		}
	}
}

func TestCanonicalKeyAndReserved(t *testing.T) {
	if CanonicalKey("Dock_Discord") != "dock-discord" {
		t.Fatalf("canonical key fold failed: %q", CanonicalKey("Dock_Discord"))
	}
	if CanonicalKey("git:commit") != "git:commit" {
		t.Fatalf("canonical key must preserve namespace: %q", CanonicalKey("git:commit"))
	}
	if !IsReserved("HELP") || !IsReserved("clear") || !IsReserved("models") {
		t.Fatal("expected help/clear/models reserved")
	}
	if IsReserved("commit") {
		t.Fatal("commit must not be reserved")
	}
}
