package hooks

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"
)

func TestHookHelperProcess(t *testing.T) {
	mode := os.Getenv("HOOK_HELPER_MODE")
	if mode == "" {
		return
	}
	_, _ = io.ReadAll(os.Stdin)
	switch mode {
	case "success":
		fmt.Print(`{"decision":"allow","message":"ok"}`)
	case "block":
		fmt.Fprint(os.Stderr, "blocked by hook")
		os.Exit(2)
	case "fail":
		fmt.Fprint(os.Stderr, "soft hook failure")
		os.Exit(1)
	case "sleep":
		time.Sleep(2 * time.Second)
		fmt.Print(`{}`)
	case "env":
		out := map[string]map[string]string{"env": {
			"HOOK_ALLOWED": os.Getenv("HOOK_ALLOWED"),
		}}
		if _, ok := os.LookupEnv("HOOK_SECRET"); ok {
			out["env"]["HOOK_SECRET"] = os.Getenv("HOOK_SECRET")
		}
		_ = json.NewEncoder(os.Stdout).Encode(out)
	case "record":
		path := os.Getenv("HOOK_RECORD_FILE")
		name := os.Getenv("HOOK_NAME")
		f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if _, err := f.WriteString(name + "\n"); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if err := f.Close(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Print(`{}`)
	case "malformed":
		fmt.Print(`{"decision":`)
	default:
		fmt.Fprintf(os.Stderr, "unknown helper mode %q", mode)
		os.Exit(1)
	}
	os.Exit(0)
}

func helperHook(mode string) CommandHook {
	return CommandHook{
		Name:    "hook-" + mode,
		Event:   EventCommand,
		Command: []string{os.Args[0], "-test.run=TestHookHelperProcess"},
		Env: map[string]string{
			"HOOK_HELPER_MODE": mode,
			"HOOK_NAME":        "hook-" + mode,
		},
	}
}

func tempRecordFile(t *testing.T) string {
	t.Helper()
	return strings.TrimRight(t.TempDir(), string(os.PathSeparator)) + string(os.PathSeparator) + "record.txt"
}
