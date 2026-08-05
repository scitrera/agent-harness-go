//go:build windows

package procgroup

import (
	"os"
	"os/exec"
	"strconv"
	"syscall"
)

// Attr gives the child its own console process group. Windows has no setpgid;
// this is the closest equivalent, and it is what bounds the taskkill tree walk
// below to our own subtree.
func Attr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP}
}

// Terminate asks the child and its descendants to close.
func Terminate(p *os.Process) error { return taskkill(p, false) }

// Kill forcibly stops the child and its descendants.
func Kill(p *os.Process) error { return taskkill(p, true) }

// Windows has no signal that addresses a process group, so the tree walk is
// delegated to taskkill. os.Process.Kill stops only the process we spawned and
// orphans everything it started, which is the failure this package exists to
// avoid — but it is still better than leaving the process running, so it is
// the fallback when taskkill is unavailable.
func taskkill(p *os.Process, force bool) error {
	if p == nil {
		return nil
	}
	args := []string{"/pid", strconv.Itoa(p.Pid), "/t"}
	if force {
		args = append(args, "/f")
	}
	if err := exec.Command("taskkill", args...).Run(); err != nil {
		return p.Kill()
	}
	return nil
}
