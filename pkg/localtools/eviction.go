package localtools

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"path"
	"strings"
)

// defaultEvictionDir is the workspace-relative dir under which evicted content is
// written. It sits under the workspace root so the model can read_file the ref.
const defaultEvictionDir = ".agent/evicted"

// EvictionSink adapts a Workspace to compaction.EvictionSink: it writes evicted
// tool-result/text content under a workspace subdir and returns the
// workspace-relative path the model reads back via read_file. It structurally
// satisfies compaction.EvictionSink (a Put method), so localtools needn't import
// compaction.
type EvictionSink struct {
	ws  *Workspace
	dir string
}

// NewEvictionSink returns a Workspace-backed eviction sink writing under dir
// (workspace-relative; "" -> defaultEvictionDir).
func NewEvictionSink(ws *Workspace, dir string) *EvictionSink {
	if strings.TrimSpace(dir) == "" {
		dir = defaultEvictionDir
	}
	return &EvictionSink{ws: ws, dir: dir}
}

// Put writes content under the sink dir keyed by a filename-safe form of key and
// returns the workspace-relative ref. Compaction hands the ref to the model as a
// read_file pointer, so it must stay inside the workspace.
func (s *EvictionSink) Put(ctx context.Context, key string, content []byte) (string, error) {
	rel := path.Join(s.dir, sanitizeEvictionKey(key))
	if err := s.ws.WriteFile(ctx, rel, string(content)); err != nil {
		return "", err
	}
	return rel, nil
}

// sanitizeEvictionKey maps an arbitrary eviction key (a tool_call id, or
// "msgID#partIndex") to a stable, filename-safe token that cannot escape the sink
// dir. Unsafe characters collapse to '_'; an empty/degenerate result falls back to
// a content-independent hash of the raw key so distinct keys stay distinct.
func sanitizeEvictionKey(key string) string {
	var b strings.Builder
	for _, r := range key {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_', r == '.':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	out := strings.Trim(b.String(), "._")
	if out == "" || out == "_" {
		sum := sha1.Sum([]byte(key))
		out = hex.EncodeToString(sum[:8])
	}
	return out + ".txt"
}
