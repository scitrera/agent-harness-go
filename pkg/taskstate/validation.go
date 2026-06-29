package taskstate

func validateState(state fileState) error {
	for _, plan := range state.Plans {
		if !validApprovalState(plan.Approval.State) {
			return ErrInvalidApproval
		}
	}
	for _, task := range state.Tasks {
		if !validTaskStatus(task.Status) {
			return ErrInvalidTransition
		}
		if !validApprovalState(task.Approval.State) {
			return ErrInvalidApproval
		}
	}
	return validateGraph(&state)
}

func validateGraph(state *fileState) error {
	for _, task := range state.Tasks {
		for _, dependencyID := range task.BlockedBy {
			dependency, ok := state.Tasks[dependencyID]
			if !ok || dependency.PlanID != task.PlanID {
				return ErrMissingDependency
			}
		}
	}
	visiting := map[string]bool{}
	visited := map[string]bool{}
	for id := range state.Tasks {
		if hasCycle(id, state.Tasks, visiting, visited) {
			return ErrDependencyCycle
		}
	}
	return nil
}

func hasCycle(id string, tasks map[string]TaskNode, visiting, visited map[string]bool) bool {
	if visited[id] {
		return false
	}
	if visiting[id] {
		return true
	}
	visiting[id] = true
	for _, dependencyID := range tasks[id].BlockedBy {
		if hasCycle(dependencyID, tasks, visiting, visited) {
			return true
		}
	}
	visiting[id] = false
	visited[id] = true
	return false
}

func dependenciesComplete(state *fileState, task TaskNode) bool {
	for _, dependencyID := range task.BlockedBy {
		dependency, ok := state.Tasks[dependencyID]
		if !ok || dependency.Status != TaskStatusCompleted {
			return false
		}
	}
	return true
}

func validTransition(from, to TaskStatus) error {
	if !validTaskStatus(from) || !validTaskStatus(to) {
		return ErrInvalidTransition
	}
	if from == to {
		return nil
	}
	switch from {
	case TaskStatusPending:
		if to == TaskStatusInProgress || to == TaskStatusCancelled {
			return nil
		}
	case TaskStatusInProgress:
		if to == TaskStatusPending || to == TaskStatusCompleted || to == TaskStatusCancelled {
			return nil
		}
	case TaskStatusCompleted, TaskStatusCancelled:
		return ErrInvalidTransition
	}
	return ErrInvalidTransition
}

func validApprovalState(state ApprovalState) bool {
	switch state {
	case ApprovalPending, ApprovalApproved, ApprovalRejected:
		return true
	default:
		return false
	}
}

func defaultStatus(status TaskStatus) TaskStatus {
	if status == "" {
		return TaskStatusPending
	}
	return status
}

func validTaskStatus(status TaskStatus) bool {
	switch status {
	case TaskStatusPending, TaskStatusInProgress, TaskStatusCompleted, TaskStatusCancelled:
		return true
	default:
		return false
	}
}
