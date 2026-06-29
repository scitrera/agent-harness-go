package taskstate

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

type FileStore struct {
	path     string
	lockPath string
	now      func() time.Time
	mu       sync.Mutex
}

func NewFileStore(path string) *FileStore {
	return &FileStore{path: path, lockPath: canonicalStatePath(path), now: time.Now}
}

func (s *FileStore) SetClock(now func() time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if now == nil {
		s.now = time.Now
		return
	}
	s.now = now
}

func (s *FileStore) currentTime() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.now == nil {
		return time.Now()
	}
	return s.now()
}

func (s *FileStore) ListPlans(ctx context.Context) ([]PlanRef, error) {
	state, err := s.load(ctx)
	if err != nil {
		return nil, err
	}
	plans := make([]PlanRef, 0, len(state.Plans))
	for _, plan := range state.Plans {
		plans = append(plans, plan)
	}
	sort.Slice(plans, func(i, j int) bool { return plans[i].ID < plans[j].ID })
	return plans, nil
}

func (s *FileStore) CreatePlan(ctx context.Context, plan PlanRef) (PlanRef, error) {
	return s.updatePlan(ctx, func(state *fileState, now time.Time) (PlanRef, error) {
		if plan.ID == "" {
			return PlanRef{}, fmt.Errorf("%w: empty id", ErrPlanNotFound)
		}
		if _, exists := state.Plans[plan.ID]; exists {
			return PlanRef{}, ErrPlanExists
		}
		plan.CreatedAt = defaultTime(plan.CreatedAt, now)
		plan.UpdatedAt = defaultTime(plan.UpdatedAt, plan.CreatedAt)
		if plan.Approval.State == "" {
			plan.Approval.State = ApprovalPending
		}
		if !validApprovalState(plan.Approval.State) {
			return PlanRef{}, ErrInvalidApproval
		}
		state.Plans[plan.ID] = plan
		return plan, nil
	})
}

func (s *FileStore) GetPlan(ctx context.Context, planID string) (PlanRef, error) {
	state, err := s.load(ctx)
	if err != nil {
		return PlanRef{}, err
	}
	plan, ok := state.Plans[planID]
	if !ok {
		return PlanRef{}, ErrPlanNotFound
	}
	return plan, nil
}

func (s *FileStore) DecidePlan(ctx context.Context, planID string, decision ApprovalDecision) (PlanRef, error) {
	return s.updatePlan(ctx, func(state *fileState, now time.Time) (PlanRef, error) {
		plan, ok := state.Plans[planID]
		if !ok {
			return PlanRef{}, ErrPlanNotFound
		}
		approval, err := approvalFromDecision(decision, now)
		if err != nil {
			return PlanRef{}, err
		}
		plan.Approval = approval
		plan.UpdatedAt = now
		state.Plans[planID] = plan
		return plan, nil
	})
}

func (s *FileStore) ListTasks(ctx context.Context, planID string) ([]TaskNode, error) {
	state, err := s.load(ctx)
	if err != nil {
		return nil, err
	}
	tasks := make([]TaskNode, 0)
	for _, task := range state.Tasks {
		if task.PlanID == planID {
			tasks = append(tasks, cloneTask(task))
		}
	}
	sortTasks(tasks)
	return tasks, nil
}

func (s *FileStore) CreateTask(ctx context.Context, task TaskNode) (TaskNode, error) {
	return s.updateTask(ctx, func(state *fileState, now time.Time) (TaskNode, error) {
		if _, ok := state.Plans[task.PlanID]; !ok {
			return TaskNode{}, ErrPlanNotFound
		}
		if task.ID == "" {
			return TaskNode{}, fmt.Errorf("%w: empty id", ErrTaskNotFound)
		}
		if _, exists := state.Tasks[task.ID]; exists {
			return TaskNode{}, ErrTaskExists
		}
		task.Status = defaultStatus(task.Status)
		if !validTaskStatus(task.Status) {
			return TaskNode{}, ErrInvalidTransition
		}
		task.Approval = defaultApproval(task.Approval)
		if !validApprovalState(task.Approval.State) {
			return TaskNode{}, ErrInvalidApproval
		}
		task.CreatedAt = defaultTime(task.CreatedAt, now)
		task.UpdatedAt = defaultTime(task.UpdatedAt, task.CreatedAt)
		state.Tasks[task.ID] = cloneTask(task)
		if err := validateGraph(state); err != nil {
			delete(state.Tasks, task.ID)
			return TaskNode{}, err
		}
		state.Events = append(state.Events, newEvent(task, "task.created", "", now))
		return cloneTask(task), nil
	})
}

func (s *FileStore) UpdateTask(ctx context.Context, update TaskUpdate) (TaskNode, error) {
	return s.updateTask(ctx, func(state *fileState, now time.Time) (TaskNode, error) {
		task, ok := state.Tasks[update.ID]
		if !ok {
			return TaskNode{}, ErrTaskNotFound
		}
		next := applyUpdate(task, update, now)
		if err := validTransition(task.Status, next.Status); err != nil {
			return TaskNode{}, err
		}
		state.Tasks[update.ID] = next
		if err := validateGraph(state); err != nil {
			state.Tasks[update.ID] = task
			return TaskNode{}, err
		}
		state.Events = append(state.Events, newEvent(next, "task.updated", next.Owner, now))
		return cloneTask(next), nil
	})
}

func (s *FileStore) UpdateTaskIfOwned(ctx context.Context, update GuardedTaskUpdate) (TaskNode, error) {
	return s.updateTask(ctx, func(state *fileState, now time.Time) (TaskNode, error) {
		task, ok := state.Tasks[update.Update.ID]
		if !ok {
			return TaskNode{}, ErrTaskNotFound
		}
		if err := validateTaskOwnerGuard(task, update.Guard); err != nil {
			return TaskNode{}, err
		}
		next := applyUpdate(task, update.Update, now)
		if err := validTransition(task.Status, next.Status); err != nil {
			return TaskNode{}, err
		}
		state.Tasks[update.Update.ID] = next
		if err := validateGraph(state); err != nil {
			state.Tasks[update.Update.ID] = task
			return TaskNode{}, err
		}
		state.Events = append(state.Events, newEvent(next, "task.updated", next.Owner, now))
		return cloneTask(next), nil
	})
}

func (s *FileStore) DecideTask(ctx context.Context, taskID string, decision ApprovalDecision) (TaskNode, error) {
	return s.updateTask(ctx, func(state *fileState, now time.Time) (TaskNode, error) {
		task, ok := state.Tasks[taskID]
		if !ok {
			return TaskNode{}, ErrTaskNotFound
		}
		approval, err := approvalFromDecision(decision, now)
		if err != nil {
			return TaskNode{}, err
		}
		task.Approval = approval
		task.UpdatedAt = now
		state.Tasks[taskID] = cloneTask(task)
		state.Events = append(state.Events, newEvent(task, "task.approval_decided", decision.Actor, now))
		return cloneTask(task), nil
	})
}

func (s *FileStore) ClaimTask(ctx context.Context, claim ClaimRequest) (TaskNode, error) {
	return s.updateTask(ctx, func(state *fileState, now time.Time) (TaskNode, error) {
		task, ok := state.Tasks[claim.TaskID]
		if !ok {
			return TaskNode{}, ErrTaskNotFound
		}
		return claimTask(state, task, claim, now)
	})
}

func (s *FileStore) ClaimNext(ctx context.Context, claim ClaimRequest) (TaskNode, error) {
	return s.updateTask(ctx, func(state *fileState, now time.Time) (TaskNode, error) {
		tasks := make([]TaskNode, 0, len(state.Tasks))
		for _, task := range state.Tasks {
			if task.PlanID == claim.PlanID {
				tasks = append(tasks, task)
			}
		}
		sortTasks(tasks)
		for _, task := range tasks {
			if claimable(state, task, claim, now) {
				claim.TaskID = task.ID
				return claimTask(state, task, claim, now)
			}
		}
		return TaskNode{}, ErrTaskBlocked
	})
}

func (s *FileStore) ListEvents(ctx context.Context, planID string) ([]TaskEvent, error) {
	state, err := s.load(ctx)
	if err != nil {
		return nil, err
	}
	events := make([]TaskEvent, 0, len(state.Events))
	for _, event := range state.Events {
		if event.PlanID == planID {
			events = append(events, event)
		}
	}
	return events, nil
}
