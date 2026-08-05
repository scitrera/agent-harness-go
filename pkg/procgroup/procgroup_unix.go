//go:build !windows

// Package procgroup starts child processes in their own process group and
// stops the whole group.
//
// Every process the harness spawns — a shell command, an MCP server — can
// spawn children of its own, so killing only the process we hold a handle to
// leaves those running. The mechanism for reaching the whole tree is entirely
// platform-specific (a negative-pid signal on Unix, a taskkill tree walk on
// Windows), which is what this package exists to hide.
package procgroup

import (
	"os"
	"syscall"
)

// Attr puts the child in its own process group so Terminate and Kill can
// address the whole tree.
func Attr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setpgid: true}
}

// Terminate asks the child's process group to shut down.
func Terminate(p *os.Process) error {
	return signalGroup(p, syscall.SIGTERM)
}

// Kill forcibly stops the child's process group.
func Kill(p *os.Process) error {
	return signalGroup(p, syscall.SIGKILL)
}

func signalGroup(p *os.Process, sig syscall.Signal) error {
	if p == nil {
		return nil
	}
	// A negative pid addresses the group Attr established.
	return syscall.Kill(-p.Pid, sig)
}
