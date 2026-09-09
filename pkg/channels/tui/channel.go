// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package tui

import (
	"context"
	"sync/atomic"

	"github.com/scitrera/agent-harness-go/pkg/channel"
)

const (
	inboxBuffer = 64
	eventBuffer = 512
)

type Channel struct {
	inbox   chan channel.Inbound
	events  chan channel.Event
	dropped atomic.Int64
}

func NewChannel() *Channel {
	return &Channel{
		inbox:  make(chan channel.Inbound, inboxBuffer),
		events: make(chan channel.Event, eventBuffer),
	}
}

func (c *Channel) Enqueue(ctx context.Context, in channel.Inbound) error {
	select {
	case c.inbox <- in:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Channel) FetchTask(ctx context.Context) (channel.Inbound, error) {
	select {
	case in := <-c.inbox:
		return in, nil
	case <-ctx.Done():
		return channel.Inbound{}, ctx.Err()
	}
}

func (c *Channel) PublishEvent(ctx context.Context, event channel.Event) error {
	if event.Type != channel.EventTokenDelta {
		select {
		case c.events <- event:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	select {
	case c.events <- event:
	default:
		c.dropped.Add(1)
	}
	return nil
}

func (c *Channel) Events() <-chan channel.Event {
	return c.events
}

func (c *Channel) DroppedEvents() int64 {
	return c.dropped.Load()
}

var (
	_ channel.Channel  = (*Channel)(nil)
	_ channel.Enqueuer = (*Channel)(nil)
)
