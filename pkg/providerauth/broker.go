// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

// Package providerauth is the Sahara OSS credential broker. It owns local
// credential profiles and login UX while delegating provider protocol details
// to go-llm.
package providerauth

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	llmauth "github.com/scitrera/go-llm/auth"
	openaiauth "github.com/scitrera/go-llm/auth/openai"
	llmclient "github.com/scitrera/go-llm/client"
)

const (
	KindOpenAISubscription = "openai_subscription"
	DefaultProfile         = "personal"
)

type Config struct {
	Store      llmauth.Store
	Dir        string
	IssuerURL  string
	ClientID   string
	Originator string
}

type Broker struct {
	store  llmauth.Store
	config openaiauth.Config
	mu     sync.Mutex
	openAI map[string]*openaiauth.Manager
}

func New(config Config) (*Broker, error) {
	store := config.Store
	if store == nil {
		var err error
		store, err = NewFileStore(config.Dir)
		if err != nil {
			return nil, err
		}
	}
	return &Broker{
		store: store, openAI: map[string]*openaiauth.Manager{},
		config: openaiauth.Config{IssuerURL: config.IssuerURL, ClientID: config.ClientID, Originator: config.Originator},
	}, nil
}

func DefaultDir() (string, error) {
	if configured := strings.TrimSpace(os.Getenv("SAHARA_AUTH_DIR")); configured != "" {
		return configured, nil
	}
	root, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("resolve user configuration directory: %w", err)
	}
	return filepath.Join(root, "sahara", "auth"), nil
}

func (b *Broker) Authenticator(kind, profile string) (llmclient.Authenticator, error) {
	if kind != KindOpenAISubscription {
		return nil, fmt.Errorf("unsupported provider auth kind %q", kind)
	}
	return b.OpenAI(profile)
}

func (b *Broker) OpenAI(profile string) (*openaiauth.Manager, error) {
	if profile == "" {
		profile = DefaultProfile
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if manager := b.openAI[profile]; manager != nil {
		return manager, nil
	}
	manager, err := openaiauth.NewManager(profile, b.store, b.config)
	if err != nil {
		return nil, err
	}
	b.openAI[profile] = manager
	return manager, nil
}

// RunCLI implements `agent-harness auth login|status|logout`. Device login is
// the default because it works in local, SSH, container, and headless sessions.
func (b *Broker) RunCLI(ctx context.Context, args []string, output io.Writer) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: agent-harness auth <login|status|logout> [--profile name]")
	}
	action := strings.ToLower(args[0])
	flags := flag.NewFlagSet("auth "+action, flag.ContinueOnError)
	flags.SetOutput(output)
	profile := flags.String("profile", DefaultProfile, "credential profile name")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected auth arguments: %s", strings.Join(flags.Args(), " "))
	}
	manager, err := b.OpenAI(*profile)
	if err != nil {
		return err
	}
	switch action {
	case "login":
		credential, err := manager.LoginDevice(ctx, func(code openaiauth.DeviceCode) {
			_, _ = fmt.Fprintf(output, "Open %s and enter code %s\n", code.VerificationURL, code.UserCode)
		})
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintf(output, "Logged in profile %q%s.\n", *profile, accountSummary(credential))
		return nil
	case "status":
		credential, err := manager.Status(ctx)
		if errors.Is(err, llmauth.ErrCredentialNotFound) {
			_, _ = fmt.Fprintf(output, "Profile %q is not logged in.\n", *profile)
			return nil
		}
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintf(output, "Profile %q is logged in%s.\n", *profile, accountSummary(credential))
		return nil
	case "logout":
		if err := manager.Logout(ctx); err != nil {
			return err
		}
		_, _ = fmt.Fprintf(output, "Logged out profile %q.\n", *profile)
		return nil
	default:
		return fmt.Errorf("unknown auth action %q; use login, status, or logout", action)
	}
}

func accountSummary(credential llmauth.Credential) string {
	parts := make([]string, 0, 2)
	if credential.Email != "" {
		parts = append(parts, credential.Email)
	}
	if credential.Plan != "" {
		parts = append(parts, credential.Plan)
	}
	if len(parts) == 0 {
		return ""
	}
	return " (" + strings.Join(parts, ", ") + ")"
}
