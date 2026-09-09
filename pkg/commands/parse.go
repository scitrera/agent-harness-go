// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package commands

import "strings"

// Parse splits a chat line into a command name and raw argument string. It
// returns ok=false when the line is not a slash command. The name is lower-cased
// and stripped of a trailing ":" ("/think:" -> "think"); args is everything
// after the first run of whitespace, trimmed. A bare "/", a leading "//", or a
// "/ foo" (slash then space) is not treated as a command.
func Parse(line string) (name, args string, ok bool) {
	trimmed := strings.TrimSpace(line)
	if !strings.HasPrefix(trimmed, "/") || trimmed == "/" {
		return "", "", false
	}
	body := trimmed[1:]
	if body == "" || body[0] == '/' || body[0] == ' ' || body[0] == '\t' {
		return "", "", false
	}
	idx := strings.IndexAny(body, " \t\n")
	if idx < 0 {
		name = body
	} else {
		name = body[:idx]
		args = strings.TrimSpace(body[idx:])
	}
	name = strings.ToLower(strings.TrimSpace(strings.TrimSuffix(name, ":")))
	if name == "" {
		return "", "", false
	}
	return name, args, true
}

// CanonicalKey normalizes a command name for lookup: lower-case with underscores
// folded to hyphens so "/dock-discord" and "/dock_discord" resolve identically.
// Namespace separators (":") are preserved.
func CanonicalKey(name string) string {
	return strings.ReplaceAll(strings.ToLower(strings.TrimSpace(name)), "_", "-")
}

// IsReserved reports whether name collides with a reserved built-in command.
func IsReserved(name string) bool {
	_, ok := ReservedNames[CanonicalKey(name)]
	return ok
}
