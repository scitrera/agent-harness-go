package tools

import (
	"context"
	"encoding/json"
	"fmt"
)

// FileListArgs are the server-side filters for a file/document listing.
type FileListArgs struct {
	Status       string
	DocumentType string
	Limit        int
	Offset       int
}

// FileListItem is one server-side file/document entry surfaced to the model. The
// materialization handle is exposed as VfsRef (the MemoryLayer document's
// source_vfs_ref) — pass it to vfs_get to fetch the file into the sandbox.
type FileListItem struct {
	VfsRef       string `json:"vfs_ref"`
	Name         string `json:"name"`
	DocID        string `json:"doc_id"`
	Mime         string `json:"mime,omitempty"`
	DocumentType string `json:"document_type,omitempty"`
	Status       string `json:"status,omitempty"`
	CreatedAt    string `json:"created_at,omitempty"`
	Size         int64  `json:"size,omitempty"`
}

// FileLister lists files/documents stored server-side in MemoryLayer for a
// workspace. Implemented by the MemoryLayer SDK adapter in the distribution;
// workspace + OBO authority are passed per call (mirrors MemoryRecaller).
type FileLister interface {
	ListFiles(ctx context.Context, auth MemoryAuthority, workspace string, opts FileListArgs) ([]FileListItem, error)
}

const listFilesToolName = "memorylayer_list_files"

// RegisterListFiles registers the memorylayer_list_files tool backed by the
// lister, with a descriptor so it is exposed via the provider tool-API.
func RegisterListFiles(reg *Registry, lister FileLister) error {
	if lister == nil {
		return fmt.Errorf("%w: file lister required", ErrInvalidTool)
	}
	if err := reg.Register(listFilesToolName, HandlerFunc(func(ctx context.Context, req Request) (Result, error) {
		return listFiles(ctx, lister, req)
	})); err != nil {
		return err
	}
	reg.Describe(Descriptor{
		Name:        listFilesToolName,
		Description: "List files/documents stored server-side in MemoryLayer for a workspace. Returns each file's vfs_ref (use vfs_get to materialize it), name, type, size, and status.",
		Parameters:  json.RawMessage(`{"type":"object","properties":{"workspace":{"type":"string","description":"Workspace id to list; defaults to the current workspace"},"document_type":{"type":"string","description":"Filter by document type: pdf|markdown|text|html|docx|pptx"},"status":{"type":"string","description":"Filter by document status"},"limit":{"type":"integer","description":"Max results (default 50)"},"offset":{"type":"integer","description":"Pagination offset"}}}`),
	})
	return nil
}

func listFiles(ctx context.Context, lister FileLister, req Request) (Result, error) {
	var args struct {
		Workspace    string `json:"workspace"`
		DocumentType string `json:"document_type"`
		Status       string `json:"status"`
		Limit        int    `json:"limit"`
		Offset       int    `json:"offset"`
	}
	// All params are optional — a no-argument call means "list the current
	// workspace with defaults". decodeArgs rejects empty arguments, so only decode
	// when the model actually passed some.
	if len(req.Arguments) > 0 {
		if err := decodeArgs(req, &args); err != nil {
			return Result{}, err
		}
	}
	ws := args.Workspace
	if ws == "" {
		ws = req.Addr.WorkspaceID
	}
	items, err := lister.ListFiles(ctx, req.Authority, ws, FileListArgs{
		Status:       args.Status,
		DocumentType: args.DocumentType,
		Limit:        args.Limit,
		Offset:       args.Offset,
	})
	if err != nil {
		return errorResult(req, err.Error())
	}
	payload, err := json.Marshal(struct {
		Workspace string         `json:"workspace"`
		Files     []FileListItem `json:"files"`
		Count     int            `json:"count"`
	}{Workspace: ws, Files: items, Count: len(items)})
	if err != nil {
		return Result{}, err
	}
	return NewJSONResult(req.CallID, req.Name, payload)
}
