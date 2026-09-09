// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package executionledger

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

type queryCursor struct {
	Version      int    `json:"v"`
	Scope        string `json:"scope"`
	AfterEventID string `json:"after_event_id"`
}

func queryScope(ref Ref, query Query) (string, error) {
	query.PageToken = ""
	query.Limit = 0
	encoded, err := json.Marshal(struct {
		Ref   Ref   `json:"ref"`
		Query Query `json:"query"`
	}{ref, query})
	if err != nil {
		return "", fmt.Errorf("executionledger: encode query scope: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func encodeCursor(cursor queryCursor) (string, error) {
	encoded, err := json.Marshal(cursor)
	if err != nil {
		return "", fmt.Errorf("executionledger: encode cursor: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(encoded), nil
}

func decodeCursor(token, scope string) (queryCursor, error) {
	encoded, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return queryCursor{}, cursorError("malformed token")
	}
	var cursor queryCursor
	if err := json.Unmarshal(encoded, &cursor); err != nil || cursor.Version != 1 || cursor.Scope != scope || cursor.AfterEventID == "" {
		return queryCursor{}, cursorError("scope or payload mismatch")
	}
	return cursor, nil
}

func cursorError(detail string) error {
	return fmt.Errorf("%w: invalid query cursor: %s", ErrInvalid, detail)
}
