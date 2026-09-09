// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

// Package commands implements OpenClaw-style slash commands for the harness.
//
// Two kinds exist:
//
//   - Workspace commands: user-authored markdown files discovered from the
//     workspace (mirroring the skills-folder model). A command file is
//     "<dir>/<name>.md", optionally namespaced one level as
//     "<dir>/<ns>/<name>.md" -> "<ns>:<name>". Its YAML-ish frontmatter carries
//     description / argument-hint / model / allowed-tools and the body is a
//     prompt template. Invoking "/<name> <args>" expands the body (substituting
//     $ARGUMENTS and $1..$9) and feeds the result to the model as the turn's
//     user prompt.
//
//   - Reserved built-ins (help, commands, clear): handled by the runner without
//     calling the model. Reserved names cannot be shadowed by workspace files.
//
// Parsing follows OpenClaw: a leading "/", an optional ":" after the name
// ("/think: high" == "/think high"), and hyphen/underscore-insensitive names.
//
// NOTE: literal OpenClaw registers commands as code handlers; a Go sandbox
// runtime cannot load handler code from a workspace, so workspace commands are
// markdown prompt-expansion (the Claude-Code convention) while parsing/dispatch
// semantics stay faithful to OpenClaw.
package commands

// Command is a single workspace command discovered from the filesystem.
type Command struct {
	Name         string   // canonical, lower-case (e.g. "commit" or "git:commit")
	Description  string   // from frontmatter description (or fallback)
	ArgumentHint string   // from frontmatter argument-hint (display only)
	Model        string   // optional per-turn model override (frontmatter model)
	AllowedTools []string // from frontmatter allowed-tools (display only for now)
	Body         string   // prompt template (frontmatter stripped)
	Path         string   // workspace-relative or absolute operator source path
}
