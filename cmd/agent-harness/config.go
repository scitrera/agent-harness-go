package main

import "time"

const shutdownTimeout = 5 * time.Second

type appConfig struct {
	workspaceRoot string
	stateDir      string
	thread        string
	baseURL       string
	model         string
	seed          bool
}
