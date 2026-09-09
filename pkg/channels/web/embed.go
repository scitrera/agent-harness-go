// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package web

import _ "embed"

// indexHTML is the self-contained single-page app (HTML + inline CSS + JS),
// served at "/" and any non-API path. No build step — it is hand-authored and
// embedded directly.
//
//go:embed assets/index.html
var indexHTML []byte
