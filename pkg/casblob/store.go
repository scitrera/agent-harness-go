// Package casblob defines the backend-neutral atomic blob contract used by
// distributed OSS state stores. A backend compares complete byte values rather
// than exposing its own revision, transaction, or lock representation.
package casblob

import "context"

// Store is the minimal persistence surface for optimistic, multi-writer state.
// Values must be treated as immutable snapshots. Create is atomic when a key is
// absent; CompareAndSwap succeeds only when the complete current value equals
// expected.
type Store interface {
	Read(ctx context.Context, key string) (value []byte, found bool, err error)
	Create(ctx context.Context, key string, value []byte) (created bool, err error)
	CompareAndSwap(ctx context.Context, key string, expected, value []byte) (swapped bool, err error)
}
