// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package localtools

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

type FileInfo struct {
	Path      string `json:"path"`
	URI       string `json:"uri"`
	Kind      string `json:"kind"`
	MediaType string `json:"media_type"`
	SizeBytes int64  `json:"size_bytes"`
	SHA256    string `json:"sha256"`
	Width     int    `json:"width,omitempty"`
	Height    int    `json:"height,omitempty"`
}

func (w *Workspace) InspectFile(ctx context.Context, relPath string) (FileInfo, error) {
	if err := ctx.Err(); err != nil {
		return FileInfo{}, err
	}
	path, err := w.resolveExisting(relPath)
	if err != nil {
		return FileInfo{}, err
	}
	stat, err := os.Stat(path)
	if err != nil {
		return FileInfo{}, fmt.Errorf("stat %s: %w", relPath, err)
	}
	if stat.IsDir() {
		return FileInfo{}, fmt.Errorf("%w: %s is a directory", ErrInvalidFile, relPath)
	}
	file, err := os.Open(path)
	if err != nil {
		return FileInfo{}, fmt.Errorf("open %s: %w", relPath, err)
	}
	defer func() { _ = file.Close() }()
	head := make([]byte, 512)
	n, readErr := io.ReadFull(file, head)
	if readErr != nil && readErr != io.EOF && readErr != io.ErrUnexpectedEOF {
		return FileInfo{}, fmt.Errorf("read header %s: %w", relPath, readErr)
	}
	hash := sha256.New()
	if n > 0 {
		_, _ = hash.Write(head[:n])
	}
	if _, err := io.Copy(hash, file); err != nil {
		return FileInfo{}, fmt.Errorf("hash %s: %w", relPath, err)
	}
	mediaType := http.DetectContentType(head[:n])
	info := FileInfo{
		Path:      cleanWorkspacePath(relPath),
		URI:       "workspace://" + cleanWorkspacePath(relPath),
		Kind:      mediaKind(mediaType),
		MediaType: mediaType,
		SizeBytes: stat.Size(),
		SHA256:    hex.EncodeToString(hash.Sum(nil)),
	}
	if strings.HasPrefix(mediaType, "image/") {
		width, height, err := imageDimensions(file)
		if err == nil {
			info.Width = width
			info.Height = height
		}
	}
	return info, nil
}

func imageDimensions(file *os.File) (int, int, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return 0, 0, fmt.Errorf("seek image: %w", err)
	}
	cfg, _, err := image.DecodeConfig(file)
	if err != nil {
		return 0, 0, fmt.Errorf("decode image config: %w", err)
	}
	return cfg.Width, cfg.Height, nil
}

func mediaKind(mediaType string) string {
	switch {
	case strings.HasPrefix(mediaType, "image/"):
		return "image"
	case strings.HasPrefix(mediaType, "audio/"):
		return "audio"
	case strings.HasPrefix(mediaType, "video/"):
		return "video"
	case strings.HasPrefix(mediaType, "text/"):
		return "text"
	default:
		return "file"
	}
}

func cleanWorkspacePath(relPath string) string {
	clean := filepath.Clean(relPath)
	if clean == "." {
		return ""
	}
	return filepath.ToSlash(clean)
}
