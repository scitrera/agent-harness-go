package taskstate

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/atomicfile"
)

var stateFileLocks sync.Map

func (s *FileStore) updatePlan(ctx context.Context, mutate func(*fileState, time.Time) (PlanRef, error)) (PlanRef, error) {
	lock := s.stateLock()
	lock.Lock()
	defer lock.Unlock()
	state, err := s.readState(ctx)
	if err != nil {
		return PlanRef{}, err
	}
	plan, err := mutate(&state, s.currentTime())
	if err != nil {
		return PlanRef{}, err
	}
	return plan, s.writeState(ctx, state)
}

func (s *FileStore) updateTask(ctx context.Context, mutate func(*fileState, time.Time) (TaskNode, error)) (TaskNode, error) {
	lock := s.stateLock()
	lock.Lock()
	defer lock.Unlock()
	state, err := s.readState(ctx)
	if err != nil {
		return TaskNode{}, err
	}
	task, err := mutate(&state, s.currentTime())
	if err != nil {
		return TaskNode{}, err
	}
	return task, s.writeState(ctx, state)
}

func (s *FileStore) load(ctx context.Context) (fileState, error) {
	lock := s.stateLock()
	lock.Lock()
	defer lock.Unlock()
	return s.readState(ctx)
}

func (s *FileStore) stateLock() *sync.Mutex {
	lockPath := s.lockPath
	if lockPath == "" {
		lockPath = canonicalStatePath(s.path)
	}
	lock, _ := stateFileLocks.LoadOrStore(lockPath, &sync.Mutex{})
	return lock.(*sync.Mutex)
}

func canonicalStatePath(path string) string {
	abs, err := filepath.Abs(path)
	if err != nil {
		return filepath.Clean(path)
	}
	dir, file := filepath.Dir(abs), filepath.Base(abs)
	resolvedDir, err := filepath.EvalSymlinks(dir)
	if err != nil {
		return filepath.Clean(abs)
	}
	return filepath.Join(resolvedDir, file)
}

func (s *FileStore) readState(ctx context.Context) (fileState, error) {
	if err := ctx.Err(); err != nil {
		return fileState{}, err
	}
	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return newFileState(), nil
		}
		return fileState{}, fmt.Errorf("read task state: %w", err)
	}
	var state fileState
	if err := json.Unmarshal(data, &state); err != nil {
		return fileState{}, fmt.Errorf("%w: %v", ErrCorruptState, err)
	}
	state.init()
	if err := validateState(state); err != nil {
		return fileState{}, err
	}
	return state, nil
}

func (s *FileStore) writeState(ctx context.Context, state fileState) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("encode task state: %w", err)
	}
	if err := atomicfile.Write(s.path, data, 0o644); err != nil {
		return fmt.Errorf("write task state: %w", err)
	}
	return nil
}
