// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package turn

import (
	"context"
	"encoding/json"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

// turnPartEmitter implements tools.PartEmitter over the turn streamer. The first
// time an id-bearing part is upserted it is streamed via part_appended (and its
// index remembered); subsequent upserts of the same id stream a part_updated
// patch. It retains the latest version of each id-bearing part so the runner can
// fold them into the finalized assistant message (so they persist). Parts
// without an id are appended each call and not tracked.
type turnPartEmitter struct {
	streamer *turnStreamer
	idIndex  map[string]int
	durable  map[string]protocol.ContentPart
	order    []string
}

func newTurnPartEmitter(s *turnStreamer) *turnPartEmitter {
	return &turnPartEmitter{
		streamer: s,
		idIndex:  map[string]int{},
		durable:  map[string]protocol.ContentPart{},
	}
}

func (e *turnPartEmitter) UpsertPart(ctx context.Context, part protocol.ContentPart) error {
	id := partID(part)
	if id == "" {
		_, err := e.streamer.appendPart(ctx, part)
		return err
	}
	if idx, seen := e.idIndex[id]; seen {
		patch, err := partFields(part)
		if err != nil {
			return err
		}
		e.durable[id] = part
		return e.streamer.updatePart(ctx, idx, patch)
	}
	idx, err := e.streamer.appendPart(ctx, part)
	if err != nil {
		return err
	}
	e.idIndex[id] = idx
	e.durable[id] = part
	e.order = append(e.order, id)
	return nil
}

// durableParts returns the latest version of each id-bearing part that was
// upserted this turn, in first-seen order, for folding into the final message.
func (e *turnPartEmitter) durableParts() []protocol.ContentPart {
	if e == nil || len(e.order) == 0 {
		return nil
	}
	out := make([]protocol.ContentPart, 0, len(e.order))
	for _, id := range e.order {
		out = append(out, e.durable[id])
	}
	return out
}

// partID extracts the top-level "id" of a content part (empty when absent).
func partID(p protocol.ContentPart) string {
	var h struct {
		ID string `json:"id"`
	}
	_ = json.Unmarshal(p.Raw(), &h)
	return h.ID
}

// partFields decodes a content part to its raw field map, used as the
// part_updated patch (the consumer reducer shallow-merges it).
func partFields(p protocol.ContentPart) (map[string]json.RawMessage, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(p.Raw(), &m); err != nil {
		return nil, err
	}
	return m, nil
}
