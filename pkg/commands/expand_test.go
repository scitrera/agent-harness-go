// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package commands

import "testing"

func TestExpand(t *testing.T) {
	cases := []struct {
		body string
		args string
		want string
	}{
		{"Run with: $ARGUMENTS", "a b c", "Run with: a b c"},
		{"first=$1 second=$2", "alpha beta", "first=alpha second=beta"},
		{"first=$1 second=$2", "alpha", "first=alpha second="},
		{"no tokens here", "ignored", "no tokens here"},
		{"$ARGUMENTS then $1", "x y", "x y then x"},
		{"empty: [$ARGUMENTS]", "", "empty: []"},
	}
	for _, c := range cases {
		if got := Expand(c.body, c.args); got != c.want {
			t.Errorf("Expand(%q, %q) = %q, want %q", c.body, c.args, got, c.want)
		}
	}
}
