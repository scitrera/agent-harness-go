package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/scitrera/agent-harness-go/pkg/channels/cli"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

func runCLI(cfg appConfig) error {
	ctx := context.Background()
	runner, _, err := buildRunner(cfg, cli.NewPublisher(os.Stdout), nil, nil)
	if err != nil {
		return err
	}

	addr := protocol.MessageAddress{ThreadID: cfg.thread}
	fmt.Fprintf(os.Stderr, "agent-harness | model=%s endpoint=%s thread=%s - type a message, Ctrl-D to exit\n", cfg.model, cfg.baseURL, cfg.thread)
	sc := bufio.NewScanner(os.Stdin)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	turnNo := 0
	for {
		fmt.Fprint(os.Stderr, "\nyou> ")
		if !sc.Scan() {
			break
		}
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		turnNo++
		part, err := protocol.NewTextPart(line)
		if err != nil {
			return err
		}
		user := protocol.ChatMessage{ID: fmt.Sprintf("user-%d", turnNo), Role: protocol.RoleUser, Addr: addr, Content: []protocol.ContentPart{part}}
		fmt.Fprint(os.Stdout, "\nassistant> ")
		if _, err := runner.Run(ctx, addr, user); err != nil {
			fmt.Fprintf(os.Stderr, "\nturn error: %v\n", err)
		}
	}
	return sc.Err()
}
