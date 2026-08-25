package tui

func (m *model) applySendResult(msg sendResultMsg) {
	if !m.workspaceMatchesCurrent(msg.WorkspaceID) {
		if msg.Err != nil {
			delete(m.turns, msg.TaskID)
		}
		return
	}
	if msg.Err != nil {
		delete(m.turns, msg.TaskID)
		m.removeThinking(msg.TaskID)
		m.addSystem("send failed: " + msg.Err.Error())
		return
	}
	m.threads = m.listThreads()
	m.status = m.activeTurnStatus()
	m.refreshViewport()
}

func (m *model) applyHistoryLoaded(msg historyLoadedMsg) {
	if !m.workspaceMatchesCurrent(msg.WorkspaceID) {
		return
	}
	if msg.Err != nil {
		m.addSystem("load history failed: " + msg.Err.Error())
		return
	}
	m.threadID = msg.ThreadID
	m.rows = rowsFromHistoryWithReasoning(msg.Messages, m.retainReasoning)
	m.renderedRows = map[string]renderedRowCache{}
	m.threads = m.listThreads()
	m.status = "thread " + msg.ThreadID
	m.tailing = true
	m.refreshViewportToBottom()
}

func (m *model) applyThreadCreated(msg threadCreatedMsg) {
	if !m.workspaceMatchesCurrent(msg.WorkspaceID) {
		return
	}
	if msg.Err != nil {
		m.addSystem("create thread failed: " + msg.Err.Error())
		return
	}
	m.threadID = msg.Session.ID
	m.rows = nil
	m.renderedRows = map[string]renderedRowCache{}
	m.threads = m.listThreads()
	m.status = "thread " + msg.Session.ID
	m.tailing = true
	m.refreshViewportToBottom()
}

func (m *model) applyThreadDeleted(msg threadDeletedMsg) {
	if !m.workspaceMatchesCurrent(msg.WorkspaceID) {
		return
	}
	if msg.Err != nil {
		m.addSystem("delete thread failed: " + msg.Err.Error())
		return
	}
	m.threads = m.listThreads()
	if m.threadID != msg.DeletedID {
		m.addSystem("deleted " + msg.DeletedID)
		return
	}
	if msg.NextID != "" {
		m.threadID = msg.NextID
		m.status = "loading " + msg.NextID
		m.rows = nil
		m.renderedRows = map[string]renderedRowCache{}
		m.refreshViewportToBottom()
		return
	}
	m.threadID = ""
	m.rows = nil
	m.renderedRows = map[string]renderedRowCache{}
	m.status = "thread deleted"
	m.refreshViewport()
}

func (m *model) applyThreadRenamed(msg threadRenamedMsg) {
	if !m.workspaceMatchesCurrent(msg.WorkspaceID) {
		return
	}
	if msg.Err != nil {
		m.addSystem("rename thread failed: " + msg.Err.Error())
		return
	}
	m.threads = m.listThreads()
	m.addSystem("renamed " + msg.ThreadID)
}

func (m *model) applyClearThread(msg clearThreadMsg) {
	delete(m.clearingThreads, workspaceKey(msg.WorkspaceID, msg.ThreadID))
	if !m.workspaceMatchesCurrent(msg.WorkspaceID) {
		return
	}
	if msg.Err != nil {
		m.addSystem("clear failed: " + msg.Err.Error())
		return
	}
	if msg.ThreadID == m.threadID {
		m.rows = nil
		m.renderedRows = map[string]renderedRowCache{}
	}
	m.addSystem("cleared " + msg.ThreadID)
}

func (m *model) applyDrawerLoaded(msg drawerLoadedMsg) {
	if msg.Err != nil {
		m.showDrawer(msg.Drawer, "load failed: "+msg.Err.Error())
		return
	}
	m.showDrawer(msg.Drawer, msg.Text)
	if msg.Status != "" {
		m.status = msg.Status
	}
}
