package team

import (
	"context"
	"fmt"
	"sort"
	"time"
)

func (s *FileGraphStore) RegisterAgent(ctx context.Context, node AgentNode) error {
	return s.update(ctx, func(state *graphState, now time.Time) error {
		node = defaultAgent(node, now)
		if err := validateAgent(node); err != nil {
			return err
		}
		if _, exists := state.Agents[node.ID]; exists {
			return ErrAgentExists
		}
		state.Agents[node.ID] = cloneAgent(node)
		return nil
	})
}

func (s *FileGraphStore) AddChild(ctx context.Context, req AddChildRequest) error {
	return s.update(ctx, func(state *graphState, now time.Time) error {
		parent, ok := state.Agents[req.ParentID]
		if !ok || parent.ID == "" {
			return ErrAgentNotFound
		}
		child := req.Child
		childExisted := false
		if existing, ok := state.Agents[child.ID]; ok {
			child = existing
			childExisted = true
		} else {
			child = defaultAgent(child, now)
			if err := validateAgent(child); err != nil {
				return err
			}
		}
		edge := AgentEdge{ParentID: req.ParentID, ChildID: child.ID, CreatedAt: now}
		if edgeExists(state.Edges, edge) {
			return nil
		}
		if req.MaxActiveChildren > 0 && activeChildren(*state, req.ParentID) >= req.MaxActiveChildren {
			return ErrConcurrencyLimit
		}
		state.Agents[child.ID] = cloneAgent(child)
		state.Edges = append(state.Edges, edge)
		if hasAgentCycle(*state, req.ParentID, child.ID) {
			state.Edges = state.Edges[:len(state.Edges)-1]
			if !childExisted {
				delete(state.Agents, child.ID)
			}
			return ErrAgentCycle
		}
		return nil
	})
}

func (s *FileGraphStore) UpdateAgentStatus(ctx context.Context, id AgentID, status AgentStatus) (AgentNode, error) {
	var updated AgentNode
	err := s.update(ctx, func(state *graphState, now time.Time) error {
		agent, ok := state.Agents[id]
		if !ok {
			return ErrAgentNotFound
		}
		if !validAgentStatus(status) {
			return ErrInvalidAgent
		}
		agent.Status = status
		agent.UpdatedAt = now
		state.Agents[id] = cloneAgent(agent)
		updated = cloneAgent(agent)
		return nil
	})
	if err != nil {
		return AgentNode{}, err
	}
	return updated, nil
}

func (s *FileGraphStore) ListAgents(ctx context.Context) ([]AgentNode, error) {
	state, err := s.load(ctx)
	if err != nil {
		return nil, err
	}
	agents := make([]AgentNode, 0, len(state.Agents))
	for _, agent := range state.Agents {
		agents = append(agents, cloneAgent(agent))
	}
	sort.Slice(agents, func(i, j int) bool { return agents[i].ID < agents[j].ID })
	return agents, nil
}

func (s *FileGraphStore) DescendantsBFS(ctx context.Context, parentID AgentID) ([]AgentNode, error) {
	state, err := s.load(ctx)
	if err != nil {
		return nil, err
	}
	if _, ok := state.Agents[parentID]; !ok {
		return nil, ErrAgentNotFound
	}
	children := childrenByParent(state.Edges)
	queue := append([]AgentID(nil), children[parentID]...)
	out := make([]AgentNode, 0, len(state.Agents))
	seen := map[AgentID]bool{parentID: true}
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		if seen[id] {
			continue
		}
		seen[id] = true
		agent, ok := state.Agents[id]
		if !ok {
			return nil, fmt.Errorf("%w: %s", ErrAgentNotFound, id)
		}
		out = append(out, cloneAgent(agent))
		queue = append(queue, children[id]...)
	}
	return out, nil
}

func validateGraphState(state graphState) error {
	for id, agent := range state.Agents {
		if id != agent.ID || validateAgent(agent) != nil {
			return ErrInvalidAgent
		}
	}
	for _, edge := range state.Edges {
		if _, ok := state.Agents[edge.ParentID]; !ok {
			return ErrAgentNotFound
		}
		if _, ok := state.Agents[edge.ChildID]; !ok {
			return ErrAgentNotFound
		}
		if hasAgentCycle(state, edge.ParentID, edge.ChildID) {
			return ErrAgentCycle
		}
	}
	return nil
}

func defaultAgent(node AgentNode, now time.Time) AgentNode {
	if node.Status == "" {
		node.Status = AgentStatusRunning
	}
	node.CreatedAt = defaultGraphTime(node.CreatedAt, now)
	node.UpdatedAt = defaultGraphTime(node.UpdatedAt, node.CreatedAt)
	return cloneAgent(node)
}

func validateAgent(node AgentNode) error {
	if node.ID == "" || node.Type == "" || !validAgentStatus(node.Status) {
		return ErrInvalidAgent
	}
	return nil
}

func validAgentStatus(status AgentStatus) bool {
	switch status {
	case AgentStatusRunning, AgentStatusCompleted, AgentStatusCancelled:
		return true
	default:
		return false
	}
}

func activeChildren(state graphState, parentID AgentID) int {
	total := 0
	for _, edge := range state.Edges {
		if edge.ParentID != parentID {
			continue
		}
		child := state.Agents[edge.ChildID]
		if child.Status == AgentStatusRunning {
			total++
		}
	}
	return total
}

func childrenByParent(edges []AgentEdge) map[AgentID][]AgentID {
	children := map[AgentID][]AgentID{}
	for _, edge := range edges {
		children[edge.ParentID] = append(children[edge.ParentID], edge.ChildID)
	}
	for parentID := range children {
		sort.Slice(children[parentID], func(i, j int) bool {
			return children[parentID][i] < children[parentID][j]
		})
	}
	return children
}

func edgeExists(edges []AgentEdge, edge AgentEdge) bool {
	for _, existing := range edges {
		if existing.ParentID == edge.ParentID && existing.ChildID == edge.ChildID {
			return true
		}
	}
	return false
}

func hasAgentCycle(state graphState, parentID, childID AgentID) bool {
	if parentID == childID {
		return true
	}
	children := childrenByParent(state.Edges)
	queue := append([]AgentID(nil), children[childID]...)
	seen := map[AgentID]bool{}
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		if id == parentID {
			return true
		}
		if seen[id] {
			continue
		}
		seen[id] = true
		queue = append(queue, children[id]...)
	}
	return false
}

func cloneAgent(agent AgentNode) AgentNode {
	if agent.Metadata != nil {
		agent.Metadata = cloneStringMap(agent.Metadata)
	}
	return agent
}

func cloneStringMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func defaultGraphTime(value, fallback time.Time) time.Time {
	if value.IsZero() {
		return fallback
	}
	return value
}
