// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package web

import (
	"os/exec"
	"runtime"
)

// OpenBrowser best-effort opens url in the default browser. It returns any
// launch error but callers typically ignore it — opening a browser is a
// convenience, not a requirement.
func OpenBrowser(url string) error {
	var name string
	var args []string
	switch runtime.GOOS {
	case "darwin":
		name, args = "open", []string{url}
	case "windows":
		name, args = "cmd", []string{"/c", "start", "", url}
	default:
		name, args = "xdg-open", []string{url}
	}
	return exec.Command(name, args...).Start()
}
