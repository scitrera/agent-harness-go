package tui

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

type referencedImage struct {
	Path string
	Size int64
}

type atReferenceSpan struct {
	start int
	end   int
	path  string
}

func (m model) resolveAtReferences(text string) (string, []referencedImage, error) {
	spans, err := parseAtReferenceSpans(text)
	if err != nil {
		return "", nil, err
	}
	if len(spans) == 0 {
		return text, nil, nil
	}
	var rewritten strings.Builder
	rewritten.Grow(len(text))
	images := make([]referencedImage, 0, len(spans))
	seenImages := make(map[string]struct{})
	cursor := 0
	imageBytes := m.attachmentBytes()
	imageCount := len(m.attachments)
	for _, span := range spans {
		target, resolveErr := resolveReferencePath(m.currentWorkingDirectory(), span.path)
		if resolveErr != nil {
			return "", nil, fmt.Errorf("@%s: %w", span.path, resolveErr)
		}
		info, statErr := os.Stat(target)
		if statErr != nil {
			return "", nil, fmt.Errorf("@%s: %w", span.path, statErr)
		}
		accessRoot := target
		if !info.IsDir() {
			accessRoot = filepath.Dir(target)
		}
		if grantErr := m.grantExternalDirectory(accessRoot); grantErr != nil {
			return "", nil, fmt.Errorf("@%s: %w", span.path, grantErr)
		}
		displayPath := filepath.ToSlash(target)
		if pathWithinWorkspace(m.workspaceRoot, target) {
			relative, relativeErr := workspaceRelativePath(m.workspaceRoot, target)
			if relativeErr != nil {
				return "", nil, fmt.Errorf("@%s: %w", span.path, relativeErr)
			}
			displayPath = relative
		}
		rewritten.WriteString(text[cursor:span.start])
		rewritten.WriteString(formatAtReference(displayPath))
		cursor = span.end

		if !info.Mode().IsRegular() || !fileIsImage(target) {
			continue
		}
		if _, duplicate := seenImages[target]; duplicate {
			continue
		}
		imageCount++
		imageBytes += info.Size()
		if imageCount > maxAttachments {
			return "", nil, fmt.Errorf("image reference limit reached (%d)", maxAttachments)
		}
		if info.Size() > maxAttachmentBytes {
			return "", nil, fmt.Errorf("@%s is %d bytes; image limit is %d", span.path, info.Size(), maxAttachmentBytes)
		}
		if imageBytes > maxTotalAttachmentBytes {
			return "", nil, fmt.Errorf("referenced and queued images exceed the %d byte total limit", maxTotalAttachmentBytes)
		}
		seenImages[target] = struct{}{}
		images = append(images, referencedImage{Path: target, Size: info.Size()})
	}
	rewritten.WriteString(text[cursor:])
	return rewritten.String(), images, nil
}

func resolveReferencePath(cwd, requested string) (string, error) {
	target, err := resolveWorkingPath(cwd, requested)
	if err == nil {
		return target, nil
	}
	trimmed := strings.TrimRight(requested, ".,;:!?)]}")
	if trimmed == "" || trimmed == requested {
		return "", err
	}
	return resolveWorkingPath(cwd, trimmed)
}

func parseAtReferenceSpans(text string) ([]atReferenceSpan, error) {
	spans := make([]atReferenceSpan, 0)
	for index := 0; index < len(text); {
		if text[index] != '@' || !pathReferenceBoundary(text, index) {
			_, size := utf8.DecodeRuneInString(text[index:])
			index += size
			continue
		}
		pathStart := index + 1
		if pathStart == len(text) {
			return nil, fmt.Errorf("path is missing after @")
		}
		if text[pathStart] == '"' || text[pathStart] == '\'' {
			quote := text[pathStart]
			pathStart++
			relativeEnd := strings.IndexByte(text[pathStart:], quote)
			if relativeEnd < 0 {
				return nil, fmt.Errorf("quoted @ path is missing its closing quote")
			}
			pathEnd := pathStart + relativeEnd
			if pathEnd == pathStart {
				return nil, fmt.Errorf("path is missing after @")
			}
			spans = append(spans, atReferenceSpan{start: index, end: pathEnd + 1, path: text[pathStart:pathEnd]})
			index = pathEnd + 1
			continue
		}
		pathEnd := pathStart
		for pathEnd < len(text) {
			value, size := utf8.DecodeRuneInString(text[pathEnd:])
			if unicodePathSpace(value) {
				break
			}
			pathEnd += size
		}
		if pathEnd == pathStart {
			return nil, fmt.Errorf("path is missing after @")
		}
		path := text[pathStart:pathEnd]
		trimmed := strings.TrimRight(path, ".,;:!?)]}")
		if trimmed != "" {
			pathEnd -= len(path) - len(trimmed)
			path = trimmed
		}
		spans = append(spans, atReferenceSpan{start: index, end: pathEnd, path: path})
		index = pathEnd
	}
	return spans, nil
}

func unicodePathSpace(value rune) bool {
	return isPathSpace(value)
}

func formatAtReference(path string) string {
	if strings.IndexFunc(path, isPathSpace) >= 0 {
		return `@"` + path + `"`
	}
	return "@" + path
}

func fileIsImage(path string) bool {
	file, err := os.Open(path)
	if err != nil {
		return false
	}
	defer func() { _ = file.Close() }()
	head := make([]byte, 512)
	read, err := io.ReadFull(file, head)
	if err != nil && err != io.ErrUnexpectedEOF {
		return false
	}
	return strings.HasPrefix(http.DetectContentType(head[:read]), "image/")
}

func appendReferencedImages(ctx context.Context, message protocol.ChatMessage, ctxImages []referencedImage) (protocol.ChatMessage, error) {
	for _, image := range ctxImages {
		attachment, err := loadResolvedImage(ctx, image.Path, image.Path)
		if err != nil {
			return message, fmt.Errorf("@%s: %w", image.Path, err)
		}
		message.Content = append(message.Content, attachment.Part)
	}
	return message, nil
}
