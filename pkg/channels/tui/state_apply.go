package tui

func (m *model) applySendResult(msg sendResultMsg) {
	if msg.Err != nil {
		m.addSystem("send failed: " + msg.Err.Error())
		return
	}
	m.threads = m.index.List()
	m.status = "queued"
	m.refreshViewport()
}

func (m *model) applyHistoryLoaded(msg historyLoadedMsg) {
	if msg.Err != nil {
		m.addSystem("load history failed: " + msg.Err.Error())
		return
	}
	m.threadID = msg.ThreadID
	m.rows = rowsFromHistory(msg.Messages)
	m.threads = m.index.List()
	m.status = "thread " + msg.ThreadID
	m.tailing = true
	m.refreshViewportToBottom()
}

func (m *model) applyThreadCreated(msg threadCreatedMsg) {
	if msg.Err != nil {
		m.addSystem("create thread failed: " + msg.Err.Error())
		return
	}
	m.threadID = msg.Session.ID
	m.rows = nil
	m.threads = m.index.List()
	m.status = "thread " + msg.Session.ID
	m.tailing = true
	m.refreshViewportToBottom()
}

func (m *model) applyThreadDeleted(msg threadDeletedMsg) {
	if msg.Err != nil {
		m.addSystem("delete thread failed: " + msg.Err.Error())
		return
	}
	m.threads = m.index.List()
	if m.threadID != msg.DeletedID {
		m.addSystem("deleted " + msg.DeletedID)
		return
	}
	if msg.NextID != "" {
		m.threadID = msg.NextID
		m.status = "loading " + msg.NextID
		m.rows = nil
		m.refreshViewportToBottom()
		return
	}
	m.threadID = ""
	m.rows = nil
	m.status = "thread deleted"
	m.refreshViewport()
}

func (m *model) applyThreadRenamed(msg threadRenamedMsg) {
	if msg.Err != nil {
		m.addSystem("rename thread failed: " + msg.Err.Error())
		return
	}
	m.threads = m.index.List()
	m.addSystem("renamed " + msg.ThreadID)
}

func (m *model) applyClearThread(msg clearThreadMsg) {
	if msg.Err != nil {
		m.addSystem("clear failed: " + msg.Err.Error())
		return
	}
	if msg.ThreadID == m.threadID {
		m.rows = nil
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
