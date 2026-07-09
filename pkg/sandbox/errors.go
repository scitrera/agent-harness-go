package sandbox

import "errors"

var (
	// ErrEmptyArgv is returned by an Executor when ExecRequest.Argv is empty.
	ErrEmptyArgv = errors.New("sandbox: empty argv")
	// ErrScriptFailed indicates a derived-operation helper script itself failed
	// (non-zero exit or unparseable output), as opposed to a well-formed
	// operation-level failure reported in the JSON contract.
	ErrScriptFailed = errors.New("sandbox: helper script failed")
	// ErrOpFailed indicates a file operation reported a failure (e.g. missing
	// path) via the JSON contract.
	ErrOpFailed = errors.New("sandbox: file operation failed")
	// ErrOldTextNotFound mirrors localtools.EditFile: the exact oldText was not
	// present in the target file.
	ErrOldTextNotFound = errors.New("sandbox: old text not found")
	// ErrOldTextAmbiguous is returned by Edit when oldText matches more than once,
	// so an exact single replacement would be ambiguous.
	ErrOldTextAmbiguous = errors.New("sandbox: old text found multiple times")
)
