// Package version carries the build identity of the agent-harness distribution.
package version

// Version is the released version of this module.
//
// It is the sync target for scitrera-repo-tools: `sync-versions` rewrites this
// line from the `agent-harness-go` entry in versions.yaml, so change it there
// rather than here or CI will report the two as drifted.
const Version = "0.0.1"

// Commit is the git revision the binary was built from. Release builds stamp it
// via `-ldflags -X`; a plain `go build` leaves the placeholder.
var Commit = "unknown"

// String renders the full build identity, e.g. `0.0.1 (a1b2c3d)`.
func String() string {
	if Commit == "" || Commit == "unknown" {
		return Version
	}
	// Release stamps carry the full 40-char SHA; short form is what people read.
	short := Commit
	if len(short) > 7 {
		short = short[:7]
	}
	return Version + " (" + short + ")"
}
