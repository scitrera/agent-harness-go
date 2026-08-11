package main

import (
	"context"
	"errors"

	aetherchan "github.com/scitrera/agent-harness-go/pkg/channels/aether"
	"github.com/scitrera/agent-harness-go/pkg/threadindex"
	"github.com/scitrera/agent-harness-go/pkg/turn"
)

type missingScheduledRunThreadLookup struct{}

func (missingScheduledRunThreadLookup) LookupWorkspaceThread(context.Context, string, string) (threadindex.Session, bool, error) {
	return threadindex.Session{}, false, nil
}

func buildAetherScheduledOperations(ch *aetherchan.Channel, st stores) (turn.ScheduledOperationsCommandProvider, error) {
	if ch == nil {
		return nil, errors.New("scheduled operations require an Aether channel")
	}
	if st.turns == nil {
		return nil, errors.New("scheduled operations require a durable turn journal")
	}
	threads, ok := st.threads.(threadindex.WorkspaceLookup)
	if !ok {
		// Task/schedule inspection remains useful with local history. A missing
		// authoritative thread lookup is represented exactly like a thread that
		// has not been created yet; no local cache is promoted to authority.
		threads = missingScheduledRunThreadLookup{}
	}
	return aetherchan.NewScheduledOperationsCommands(ch, st.turns, threads, nil, 0)
}
