package taskstate

type fileState struct {
	Plans  map[string]PlanRef  `json:"plans"`
	Tasks  map[string]TaskNode `json:"tasks"`
	Events []TaskEvent         `json:"events,omitempty"`
}

func newFileState() fileState {
	return fileState{
		Plans: map[string]PlanRef{},
		Tasks: map[string]TaskNode{},
	}
}

func (s *fileState) init() {
	if s.Plans == nil {
		s.Plans = map[string]PlanRef{}
	}
	for id, plan := range s.Plans {
		plan.Approval = defaultApproval(plan.Approval)
		s.Plans[id] = plan
	}
	if s.Tasks == nil {
		s.Tasks = map[string]TaskNode{}
	}
	for id, task := range s.Tasks {
		task.Status = defaultStatus(task.Status)
		task.Approval = defaultApproval(task.Approval)
		s.Tasks[id] = task
	}
}
