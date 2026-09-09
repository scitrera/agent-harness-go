// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package tui

import (
	"strings"
	"testing"
	"time"

	"charm.land/bubbles/v2/viewport"
	tea "charm.land/bubbletea/v2"
)

func TestUpdateKeyRequiresSameQuitKeyTwice(t *testing.T) {
	m := model{
		status:   "ready",
		viewport: viewport.New(),
		composer: newComposer(),
		tailing:  true,
	}
	ctrlC := tea.KeyPressMsg(tea.Key{Code: 'c', Mod: tea.ModCtrl})
	ctrlD := tea.KeyPressMsg(tea.Key{Code: 'd', Mod: tea.ModCtrl})

	next, firstCmd := m.updateKey(ctrlC)
	if firstCmd == nil {
		t.Fatal("first Ctrl+C should schedule confirmation expiry")
	}
	updated := next.(model)
	if !strings.Contains(updated.status, "Ctrl+C again") {
		t.Fatalf("first Ctrl+C status = %q", updated.status)
	}

	next, mixedCmd := updated.updateKey(ctrlD)
	if mixedCmd == nil {
		t.Fatal("different quit key should arm a new confirmation")
	}
	updated = next.(model)
	if !strings.Contains(updated.status, "Ctrl+D again") {
		t.Fatalf("mixed-key status = %q", updated.status)
	}

	next, quitCmd := updated.updateKey(ctrlD)
	if quitCmd == nil {
		t.Fatal("second Ctrl+D should quit")
	}
	if _, ok := quitCmd().(tea.QuitMsg); !ok {
		t.Fatalf("second Ctrl+D command result = %T, want QuitMsg", quitCmd())
	}
	updated = next.(model)
	if updated.quitConfirmation.Key != "" || updated.status != "ready" {
		t.Fatalf("quit confirmation was not cleared: %+v status=%q", updated.quitConfirmation, updated.status)
	}
}

func TestQuitConfirmationExpiresAndNormalInputCancelsIt(t *testing.T) {
	m := model{
		status:   "ready",
		viewport: viewport.New(),
		composer: newComposer(),
		tailing:  true,
	}
	next, _ := m.handleQuitKey("ctrl+c", time.Now())
	updated := next.(model)
	token := updated.quitConfirmation.Token
	updated.expireQuitConfirmation(quitConfirmationExpiredMsg{Token: token})
	if updated.quitConfirmation.Key != "" || updated.status != "ready" {
		t.Fatalf("expired confirmation = %+v status=%q", updated.quitConfirmation, updated.status)
	}

	next, _ = updated.handleQuitKey("ctrl+c", time.Now())
	updated = next.(model)
	next, _ = updated.updateKey(tea.KeyPressMsg(tea.Key{Code: 'x', Text: "x"}))
	updated = next.(model)
	if updated.quitConfirmation.Key != "" || updated.status != "ready" {
		t.Fatalf("normal input did not cancel confirmation: %+v status=%q", updated.quitConfirmation, updated.status)
	}
	if got := updated.composer.Value(); got != "x" {
		t.Fatalf("normal input after confirmation = %q", got)
	}
}
