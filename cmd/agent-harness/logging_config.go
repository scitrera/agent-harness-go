// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package main

import (
	"os"
	"path/filepath"

	"github.com/scitrera/agent-harness-go/pkg/logging"
)

func setupAppLogging(workspaceRoot string, tuiMode bool) error {
	return logging.Setup(resolveLogConfig(workspaceRoot, tuiMode))
}

func resolveLogConfig(workspaceRoot string, tuiMode bool) logging.Config {
	destination := env("SAHARA_LOG_DESTINATION", "console")
	if tuiMode && os.Getenv("SAHARA_LOG_DESTINATION") == "" {
		destination = "file"
	}
	return logging.Config{
		Destination: destination,
		File:        env("SAHARA_LOG_FILE", filepath.Join(workspaceRoot, "logs", "agent-harness.log")),
		Level:       env("SAHARA_LOG_LEVEL", "info"),
		Format:      env("SAHARA_LOG_FORMAT", "text"),
	}
}
