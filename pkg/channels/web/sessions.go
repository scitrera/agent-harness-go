package web

import "github.com/scitrera/agent-harness-go/pkg/threadindex"

type Session = threadindex.Session
type Index = threadindex.Index

var NewIndex = threadindex.NewIndex

func randID(prefix string) (string, error) {
	return threadindex.NewID(prefix)
}
