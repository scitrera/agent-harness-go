package memorylayer

import (
	"context"
	"fmt"

	memorylayersdk "github.com/scitrera/memorylayer/memorylayer-sdk-go"

	"github.com/scitrera/agent-harness-go/pkg/promptnotes"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

const (
	promptNotePageSize = 100
	maxPromptNotePages = 5
)

// PromptNoteProvider loads the current, workspace-scoped prompt-note heads via
// MemoryLayer's typed Go SDK. The server remains the sole authority; this
// adapter neither caches nor falls back to local files.
type PromptNoteProvider struct {
	client *memorylayersdk.Client
}

type promptNoteProviderConfig struct {
	transport memorylayersdk.Transport
}

// PromptNoteProviderOption configures the typed prompt-note client without
// changing the legacy chat/catalog Config used elsewhere in this package.
type PromptNoteProviderOption func(*promptNoteProviderConfig)

// WithPromptNoteTransport routes prompt-note requests over a MemoryLayer SDK
// transport such as the Aether proxy transport. When set, Config.BaseURL is not
// used for request delivery.
func WithPromptNoteTransport(transport memorylayersdk.Transport) PromptNoteProviderOption {
	return func(config *promptNoteProviderConfig) { config.transport = transport }
}

// NewPromptNoteProvider constructs a typed MemoryLayer prompt-note adapter.
func NewPromptNoteProvider(cfg Config, providerOptions ...PromptNoteProviderOption) (*PromptNoteProvider, error) {
	providerConfig := promptNoteProviderConfig{}
	for _, option := range providerOptions {
		if option != nil {
			option(&providerConfig)
		}
	}
	opts := []memorylayersdk.Option{
		memorylayersdk.WithAPIKey(cfg.APIKey),
		memorylayersdk.WithWorkspaceID(cfg.Workspace),
	}
	if providerConfig.transport != nil {
		opts = append(opts, memorylayersdk.WithTransport(providerConfig.transport))
	} else {
		if cfg.BaseURL == "" {
			return nil, fmt.Errorf("memorylayer: prompt notes base url or transport required")
		}
		opts = append(opts, memorylayersdk.WithBaseURL(cfg.BaseURL))
	}
	if cfg.HTTPClient != nil && providerConfig.transport == nil {
		opts = append(opts, memorylayersdk.WithHTTPClient(cfg.HTTPClient))
	}
	client, err := memorylayersdk.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("memorylayer: prompt notes client: %w", err)
	}
	return &PromptNoteProvider{client: client}, nil
}

// LoadWorkspace retrieves all current heads within a bounded five-page window.
// Exceeding that operational bound is explicit rather than silently truncating
// authoritative guidance.
func (p *PromptNoteProvider) LoadWorkspace(ctx context.Context, workspaceID string) ([]promptnotes.Note, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	authority := promptNoteAuthority(ctx)
	pageToken := ""
	seenTokens := map[string]struct{}{}
	var notes []promptnotes.Note
	for pageNumber := 0; pageNumber < maxPromptNotePages; pageNumber++ {
		page, err := p.client.PromptNotes.List(ctx, memorylayersdk.PromptNoteListOptions{
			WorkspaceID: workspaceID,
			Limit:       promptNotePageSize,
			PageToken:   pageToken,
			Authority:   authority,
		})
		if err != nil {
			return nil, fmt.Errorf("memorylayer: list prompt notes: %w", err)
		}
		for _, note := range page.Notes {
			notes = append(notes, promptnotes.Note{
				ID: note.ID, Key: note.Key, Title: note.Title, Content: note.Content,
				Enabled: note.Enabled, SchemaVersion: note.SchemaVersion, Metadata: note.Metadata,
				Revision: note.Revision, ETag: note.ETag,
			})
		}
		if page.NextPageToken == "" {
			return notes, nil
		}
		if _, repeated := seenTokens[page.NextPageToken]; repeated {
			return nil, fmt.Errorf("memorylayer: prompt-note cursor cycle at %q", page.NextPageToken)
		}
		seenTokens[page.NextPageToken] = struct{}{}
		pageToken = page.NextPageToken
	}
	return nil, fmt.Errorf("memorylayer: prompt-note listing exceeds %d pages (%d heads maximum)", maxPromptNotePages, promptNotePageSize*maxPromptNotePages)
}

func promptNoteAuthority(ctx context.Context) *memorylayersdk.AuthorityContext {
	auth, ok := tools.MemoryAuthorityFrom(ctx)
	if !ok || auth.GrantID == "" {
		return nil
	}
	out := &memorylayersdk.AuthorityContext{GrantID: auth.GrantID}
	if auth.SubjectID != "" {
		out.Subject = &memorylayersdk.PrincipalRef{Type: auth.SubjectType, ID: auth.SubjectID}
	}
	return out
}

var _ promptnotes.WorkspaceProvider = (*PromptNoteProvider)(nil)
