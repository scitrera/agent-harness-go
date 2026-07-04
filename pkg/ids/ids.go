// Package ids generates short random identifiers for threads, sessions, and
// similar keys. One implementation shared by the UI transports and the turn
// runner (previously hand-rolled separately in threadindex and turn).
package ids

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

// New returns prefix followed by a random hex token. The error is essentially
// never returned (crypto/rand.Read failing is catastrophic) but is surfaced so
// callers can decide how to react.
func New(prefix string) (string, error) {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("random id: %w", err)
	}
	return prefix + hex.EncodeToString(b), nil
}
