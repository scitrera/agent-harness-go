package tui

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	tea "charm.land/bubbletea/v2"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

const (
	maxAttachmentBytes      = 8 << 20
	maxTotalAttachmentBytes = 16 << 20
	maxAttachments          = 8
)

func (m model) handleAttachments(fields []string) (tea.Model, tea.Cmd) {
	if len(fields) >= 2 && fields[1] == "clear" {
		m.attachments = nil
		m.updateAttachmentPlaceholder()
		m.showDrawer(drawerExtensibility, "attachments\nqueue cleared")
		return m, nil
	}
	if fields[0] == "/attachments" || len(fields) < 2 {
		m.showDrawer(drawerExtensibility, m.attachmentSummary())
		return m, nil
	}
	if len(m.attachments) >= maxAttachments {
		m.addSystem(fmt.Sprintf("attachment limit reached (%d)", maxAttachments))
		return m, nil
	}
	path := trimMatchingQuotes(strings.Join(fields[1:], " "))
	target, err := m.resolveAttachmentTarget(path)
	if err != nil {
		m.addSystem("attach failed: " + err.Error())
		return m, nil
	}
	m.status = "loading attachment"
	return m, loadAttachmentCmd(m.ctx, target)
}

func (m model) updatePaste(msg tea.PasteMsg) (tea.Model, tea.Cmd) {
	if path, ok := m.pastedImagePath(msg.Content); ok {
		if len(m.attachments) >= maxAttachments {
			m.addSystem(fmt.Sprintf("attachment limit reached (%d)", maxAttachments))
			return m, nil
		}
		m.status = "loading attachment"
		return m, loadAttachmentCmd(m.ctx, path)
	}
	var cmd tea.Cmd
	m.composer, cmd = m.composer.Update(msg)
	m.refreshInputSurface()
	return m, cmd
}

func (m model) pastedImagePath(content string) (string, bool) {
	if strings.TrimSpace(m.composer.Value()) != "" || strings.ContainsAny(content, "\r\n") {
		return "", false
	}
	requested := trimMatchingQuotes(strings.TrimSpace(content))
	switch strings.ToLower(filepath.Ext(requested)) {
	case ".avif", ".gif", ".jpeg", ".jpg", ".png", ".webp":
	default:
		return "", false
	}
	target, err := m.resolveAttachmentTarget(requested)
	if err != nil {
		return "", false
	}
	info, err := os.Stat(target)
	if err != nil || !info.Mode().IsRegular() {
		return "", false
	}
	return target, true
}

func (m model) resolveAttachmentTarget(requested string) (string, error) {
	target, err := resolveWorkingPath(m.currentWorkingDirectory(), requested)
	if err != nil {
		return "", err
	}
	if err := m.grantExternalDirectory(filepath.Dir(target)); err != nil {
		return "", err
	}
	return target, nil
}

func (m *model) applyAttachmentLoaded(msg attachmentLoadedMsg) {
	if msg.Err != nil {
		m.addSystem("attach failed: " + msg.Err.Error())
		return
	}
	if len(m.attachments) >= maxAttachments {
		m.addSystem(fmt.Sprintf("attachment limit reached (%d)", maxAttachments))
		return
	}
	if m.attachmentBytes()+msg.Attachment.Size > maxTotalAttachmentBytes {
		m.addSystem(fmt.Sprintf("queued attachments exceed the %d byte total limit", maxTotalAttachmentBytes))
		return
	}
	m.attachments = append(m.attachments, msg.Attachment)
	m.status = "attached " + msg.Attachment.Name
	m.updateAttachmentPlaceholder()
	m.reflowSurfaces()
	m.refreshViewport()
}

func (m model) attachmentBytes() int64 {
	var total int64
	for _, attachment := range m.attachments {
		total += attachment.Size
	}
	return total
}

func trimMatchingQuotes(value string) string {
	if len(value) < 2 {
		return value
	}
	first := value[0]
	if (first == '"' || first == '\'') && value[len(value)-1] == first {
		return value[1 : len(value)-1]
	}
	return value
}

func (m *model) updateAttachmentPlaceholder() {
	if len(m.attachments) == 0 {
		m.composer.Placeholder = defaultComposerPlaceholder
		return
	}
	m.composer.Placeholder = fmt.Sprintf("Message (%d image attached)", len(m.attachments))
}

func (m model) attachmentSummary() string {
	if len(m.attachments) == 0 {
		return "attachments\nnone queued\nusage: /attach <image-path>"
	}
	lines := []string{"queued attachments"}
	for _, attachment := range m.attachments {
		lines = append(lines, fmt.Sprintf("%s %s %d bytes", attachment.Name, attachment.Mime, attachment.Size))
	}
	lines = append(lines, "/attach clear to remove all")
	return strings.Join(lines, "\n")
}

func loadAttachmentCmd(ctx context.Context, target string) tea.Cmd {
	return func() tea.Msg {
		attachment, err := loadResolvedImage(ctx, target, target)
		return attachmentLoadedMsg{Attachment: attachment, Err: err}
	}
}

func loadWorkspaceImage(ctx context.Context, workspaceRoot, requestedPath string) (pendingAttachment, error) {
	return loadWorkspaceImageFrom(ctx, workspaceRoot, workspaceRoot, requestedPath)
}

func loadWorkspaceImageFrom(ctx context.Context, workspaceRoot, cwd, requestedPath string) (pendingAttachment, error) {
	if err := ctx.Err(); err != nil {
		return pendingAttachment{}, err
	}
	root, target, err := resolveWorkspacePath(workspaceRoot, cwd, requestedPath)
	if err != nil {
		return pendingAttachment{}, err
	}
	displayPath, relativeErr := filepath.Rel(root, target)
	if relativeErr != nil {
		displayPath = target
	}
	return loadResolvedImage(ctx, target, displayPath)
}

func loadResolvedImage(ctx context.Context, target, displayPath string) (pendingAttachment, error) {
	if err := ctx.Err(); err != nil {
		return pendingAttachment{}, err
	}
	info, err := os.Stat(target)
	if err != nil {
		return pendingAttachment{}, err
	}
	if !info.Mode().IsRegular() {
		return pendingAttachment{}, fmt.Errorf("attachment is not a regular file")
	}
	if info.Size() > maxAttachmentBytes {
		return pendingAttachment{}, fmt.Errorf("attachment is %d bytes; limit is %d", info.Size(), maxAttachmentBytes)
	}
	data, err := os.ReadFile(target)
	if err != nil {
		return pendingAttachment{}, err
	}
	if err := ctx.Err(); err != nil {
		return pendingAttachment{}, err
	}
	mimeType := http.DetectContentType(data)
	if !strings.HasPrefix(mimeType, "image/") {
		return pendingAttachment{}, fmt.Errorf("%s is %s; only images are supported", displayPath, mimeType)
	}
	name := filepath.Base(target)
	part, err := protocol.NewImagePart(protocol.ImagePart{
		Mime:    mimeType,
		DataURI: "data:" + mimeType + ";base64," + base64.StdEncoding.EncodeToString(data),
		AltText: name,
	})
	if err != nil {
		return pendingAttachment{}, err
	}
	return pendingAttachment{Name: name, Mime: mimeType, Size: info.Size(), Part: part}, nil
}
