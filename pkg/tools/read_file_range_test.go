package tools

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/localtools"
)

func TestNumberedLineSlice(t *testing.T) {
	const text = "alpha\nbeta\ngamma\ndelta\n" // trailing newline -> 4 lines
	cases := []struct {
		name       string
		start, end int
		want       string
	}{
		{"middle range", 2, 3, "     2\tbeta\n     3\tgamma"},
		{"clamp end past EOF", 3, 99, "     3\tgamma\n     4\tdelta"},
		{"start clamps to 1", 0, 2, "     1\talpha\n     2\tbeta"},
		{"end omitted -> to EOF", 4, 0, "     4\tdelta"},
		{"start past EOF -> empty", 99, 0, ""},
		{"start > end -> empty", 3, 2, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := numberedLineSlice(text, tc.start, tc.end); got != tc.want {
				t.Fatalf("numberedLineSlice(%d,%d) =\n%q\nwant\n%q", tc.start, tc.end, got, tc.want)
			}
		})
	}
}

func TestReadFileLineRange(t *testing.T) {
	ctx := context.Background()
	ws, err := localtools.NewWorkspace(t.TempDir())
	if err != nil {
		t.Fatalf("workspace: %v", err)
	}
	if err := ws.WriteFile(ctx, "notes.txt", "one\ntwo\nthree\nfour\nfive\n"); err != nil {
		t.Fatalf("write: %v", err)
	}
	reg := NewRegistry()
	if err := RegisterLocal(reg, LocalConfig{Workspace: ws}); err != nil {
		t.Fatalf("register: %v", err)
	}
	var rd struct {
		Text string `json:"text"`
	}

	// Ranged read -> line-number prefixed slice.
	res, err := reg.Invoke(ctx, Request{CallID: "c1", Name: "read_file",
		Arguments: json.RawMessage(`{"path":"notes.txt","start_line":2,"end_line":4}`)})
	if err != nil {
		t.Fatalf("invoke ranged: %v", err)
	}
	if err := json.Unmarshal(res.Payload, &rd); err != nil {
		t.Fatalf("unmarshal ranged: %v", err)
	}
	if rd.Text != "     2\ttwo\n     3\tthree\n     4\tfour" {
		t.Fatalf("ranged read = %q", rd.Text)
	}

	// No range -> raw full content, unchanged and NOT line-numbered.
	res2, err := reg.Invoke(ctx, Request{CallID: "c2", Name: "read_file",
		Arguments: json.RawMessage(`{"path":"notes.txt"}`)})
	if err != nil {
		t.Fatalf("invoke full: %v", err)
	}
	if err := json.Unmarshal(res2.Payload, &rd); err != nil {
		t.Fatalf("unmarshal full: %v", err)
	}
	if rd.Text != "one\ntwo\nthree\nfour\nfive\n" || strings.Contains(rd.Text, "\t") {
		t.Fatalf("full read altered: %q", rd.Text)
	}
}
