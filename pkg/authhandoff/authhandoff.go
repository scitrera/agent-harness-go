// Package authhandoff provides a small, in-process, single-use token→authority
// registry. It exists so a TRUSTED in-process producer — a background sub-agent's
// completion, which needs to wake a fresh parent turn — can hand the parent's OBO
// authority to that turn WITHOUT carrying any credential in the (unauthenticated)
// inbound message. The message carries only an opaque token; the runtime resolves
// it here.
//
// Security: tokens are unguessable (crypto/rand) and single-use (consumed on
// Resolve), and the store only ever holds authorities the harness itself minted
// for in-process handoffs. So even a forged inbound cannot escalate — an unknown
// token is a miss (zero authority), and a lucky hit would only ever return an
// authority the harness had already authorized. This mirrors turncancel.Canceller
// (a dependency-free leaf keyed by an id) and avoids trusting OBO read off the
// message payload, which the gateway — not the harness — is the sole authority for.
package authhandoff

import (
	"crypto/rand"
	"encoding/hex"
	"sync"

	"github.com/scitrera/agent-harness-go/pkg/tools"
)

// Store maps single-use tokens to a snapshot of a parent turn's OBO authority.
type Store struct {
	mu sync.Mutex
	m  map[string]tools.MemoryAuthority
}

// New returns a ready Store.
func New() *Store { return &Store{m: map[string]tools.MemoryAuthority{}} }

// Put stores auth under a fresh random token and returns the token. A nil Store,
// or a zero authority (nothing to hand off, e.g. local dev with no OBO), returns
// "" — the caller then omits the token from the inbound entirely.
func (s *Store) Put(auth tools.MemoryAuthority) string {
	if s == nil || auth == (tools.MemoryAuthority{}) {
		return ""
	}
	tok := newToken()
	s.mu.Lock()
	s.m[tok] = auth
	s.mu.Unlock()
	return tok
}

// Resolve consumes the token and returns the stored authority. ok is false when
// the token is absent (unknown, forged, or already consumed). A nil Store or an
// empty token is a miss. Single-use: a resolved token is deleted, so a re-driven
// turn falls through to the caller's default derivation rather than replaying a
// stale grant.
func (s *Store) Resolve(token string) (auth tools.MemoryAuthority, ok bool) {
	if s == nil || token == "" {
		return tools.MemoryAuthority{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	auth, ok = s.m[token]
	if ok {
		delete(s.m, token)
	}
	return auth, ok
}

func newToken() string {
	var b [32]byte
	// crypto/rand.Read never returns a short read/error on supported platforms;
	// a zeroed token would still be a valid (if unlucky) opaque key.
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
