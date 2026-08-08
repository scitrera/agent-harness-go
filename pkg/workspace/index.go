package workspace

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/scitrera/agent-harness-go/pkg/atomicfile"
)

const indexVersion = 1

// ErrInvalidIndex indicates that the persisted workspace index cannot be used
// safely. Callers should surface the error instead of silently replacing the
// index and changing existing project identities.
var ErrInvalidIndex = errors.New("invalid workspace index")

// ProjectKind describes the local evidence used to identify a project root.
// It is intentionally local metadata, not part of the ecosystem wire protocol.
type ProjectKind string

const (
	ProjectKindGit       ProjectKind = "git"
	ProjectKindDirectory ProjectKind = "directory"
)

// Entry is a durable canonical-project-root to workspace-ID assignment.
type Entry struct {
	WorkspaceID string      `json:"workspace_id"`
	ProjectRoot string      `json:"project_root"`
	Kind        ProjectKind `json:"kind"`
}

type indexDocument struct {
	Version int     `json:"version"`
	Entries []Entry `json:"entries"`
}

type projectIndex struct {
	path string

	mu     sync.Mutex
	byPath map[string]Entry
	byID   map[string]Entry
}

func newProjectIndex(stateDir string) (*projectIndex, error) {
	x := &projectIndex{
		path:   filepath.Join(stateDir, "workspaces", "index.json"),
		byPath: make(map[string]Entry),
		byID:   make(map[string]Entry),
	}
	if err := x.load(); err != nil {
		return nil, err
	}
	return x, nil
}

func (x *projectIndex) load() error {
	data, err := os.ReadFile(x.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read workspace index: %w", err)
	}

	var doc indexDocument
	if err := json.Unmarshal(data, &doc); err != nil {
		return fmt.Errorf("%w: decode %s: %v", ErrInvalidIndex, x.path, err)
	}
	if doc.Version != indexVersion {
		return fmt.Errorf("%w: unsupported version %d", ErrInvalidIndex, doc.Version)
	}
	for i, entry := range doc.Entries {
		if err := validateEntry(entry); err != nil {
			return fmt.Errorf("%w: entry %d: %v", ErrInvalidIndex, i, err)
		}
		if previous, ok := x.byPath[entry.ProjectRoot]; ok {
			return fmt.Errorf("%w: project root %q maps to both %q and %q", ErrInvalidIndex, entry.ProjectRoot, previous.WorkspaceID, entry.WorkspaceID)
		}
		if previous, ok := x.byID[entry.WorkspaceID]; ok {
			return fmt.Errorf("%w: workspace ID %q maps to both %q and %q", ErrInvalidIndex, entry.WorkspaceID, previous.ProjectRoot, entry.ProjectRoot)
		}
		x.byPath[entry.ProjectRoot] = entry
		x.byID[entry.WorkspaceID] = entry
	}
	return nil
}

func validateEntry(entry Entry) error {
	if strings.TrimSpace(entry.WorkspaceID) == "" {
		return errors.New("workspace ID is empty")
	}
	if strings.TrimSpace(entry.ProjectRoot) == "" {
		return errors.New("project root is empty")
	}
	if entry.Kind != ProjectKindGit && entry.Kind != ProjectKindDirectory {
		return fmt.Errorf("unknown project kind %q", entry.Kind)
	}
	return nil
}

func (x *projectIndex) assign(projectRoot string, kind ProjectKind) (Entry, error) {
	x.mu.Lock()
	defer x.mu.Unlock()

	if entry, ok := x.byPath[projectRoot]; ok {
		if entry.Kind != kind {
			entry.Kind = kind
			entries := x.entriesLocked()
			for i := range entries {
				if entries[i].ProjectRoot == projectRoot {
					entries[i] = entry
					break
				}
			}
			if err := x.saveLocked(entries); err != nil {
				return Entry{}, err
			}
			x.byPath[projectRoot] = entry
			x.byID[entry.WorkspaceID] = entry
		}
		return entry, nil
	}

	workspaceID := x.availableIDLocked(projectRoot)
	entry := Entry{WorkspaceID: workspaceID, ProjectRoot: projectRoot, Kind: kind}
	entries := x.entriesLocked()
	entries = append(entries, entry)
	if err := x.saveLocked(entries); err != nil {
		return Entry{}, err
	}
	x.byPath[projectRoot] = entry
	x.byID[workspaceID] = entry
	return entry, nil
}

func (x *projectIndex) availableIDLocked(projectRoot string) string {
	base := projectSlug(filepath.Base(projectRoot))
	if _, exists := x.byID[base]; !exists {
		return base
	}

	sum := sha256.Sum256([]byte(projectRoot))
	digest := hex.EncodeToString(sum[:])
	for length := 8; length <= len(digest); length += 4 {
		candidate := base + "-" + digest[:length]
		if _, exists := x.byID[candidate]; !exists {
			return candidate
		}
	}

	// A full SHA-256 collision is not a practical case, but retaining a
	// deterministic final probe keeps the index total even if it is hand-edited.
	for n := 2; ; n++ {
		candidate := fmt.Sprintf("%s-%s-%d", base, digest, n)
		if _, exists := x.byID[candidate]; !exists {
			return candidate
		}
	}
}

func (x *projectIndex) saveLocked(entries []Entry) error {
	sortEntries(entries)
	data, err := json.MarshalIndent(indexDocument{Version: indexVersion, Entries: entries}, "", "  ")
	if err != nil {
		return fmt.Errorf("encode workspace index: %w", err)
	}
	data = append(data, '\n')
	if err := atomicfile.Write(x.path, data, 0o644); err != nil {
		return fmt.Errorf("save workspace index: %w", err)
	}
	return nil
}

func (x *projectIndex) entriesLocked() []Entry {
	entries := make([]Entry, 0, len(x.byID))
	for _, entry := range x.byID {
		entries = append(entries, entry)
	}
	return entries
}

func (x *projectIndex) list() []Entry {
	x.mu.Lock()
	defer x.mu.Unlock()
	entries := x.entriesLocked()
	sortEntries(entries)
	return entries
}

func sortEntries(entries []Entry) {
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].WorkspaceID < entries[j].WorkspaceID
	})
}

func projectSlug(name string) string {
	name = strings.ToLower(strings.TrimSpace(name))
	var b strings.Builder
	b.Grow(len(name))
	separator := false
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			if separator && b.Len() > 0 {
				b.WriteByte('-')
			}
			b.WriteRune(r)
			separator = false
		default:
			separator = true
		}
	}
	slug := strings.Trim(b.String(), "-")
	if slug == "" {
		slug = "project"
	}
	const maxSlugLength = 48
	if len(slug) > maxSlugLength {
		slug = strings.TrimRight(slug[:maxSlugLength], "-")
	}
	return slug
}
