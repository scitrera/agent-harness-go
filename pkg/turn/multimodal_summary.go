// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package turn

import "github.com/scitrera/agent-harness-go/pkg/protocol"

// multimodalStats counts image/file content parts across a set of messages,
// bucketed by carrier. "unresolved" parts (vfs_ref-only or no carrier at all)
// are not deliverable to the model because the harness has no VFS resolver, so
// they are dropped during provider lowering — tracking them separately is what
// makes a missing-attachment turn debuggable from the logs.
type multimodalStats struct {
	images     int
	files      int
	dataURI    int
	uri        int
	vfsRef     int
	unresolved int
}

func (s multimodalStats) any() bool { return s.images > 0 || s.files > 0 }

// tally buckets one part by the strongest carrier it offers (inline data first,
// then a fetchable uri, then a vfs_ref the harness can't currently resolve).
func (s *multimodalStats) tally(dataURI, uri, vfsRef string) {
	switch {
	case dataURI != "":
		s.dataURI++
	case uri != "":
		s.uri++
	case vfsRef != "":
		s.vfsRef++
		s.unresolved++
	default:
		s.unresolved++
	}
}

func summarizeMultimodal(messages []protocol.ChatMessage) multimodalStats {
	var s multimodalStats
	for _, m := range messages {
		for _, p := range m.Content {
			switch p.Type() {
			case protocol.ContentImage:
				if img, ok := p.AsImage(); ok {
					s.images++
					s.tally(img.DataURI, img.URI, img.VFSRef)
				}
			case protocol.ContentFile:
				if f, ok := p.AsFile(); ok {
					s.files++
					// FilePart has no inline data_uri carrier.
					s.tally("", f.URI, f.VFSRef)
				}
			}
		}
	}
	return s
}
