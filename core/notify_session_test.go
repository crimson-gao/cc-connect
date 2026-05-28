package core

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type notifyTestPlatform struct {
	stubPlatformEngine
	mu              sync.Mutex
	reconstruct     bool
	notifications   []NotifyRequest
	reconstructed   []string
	notificationErr error
}

func (p *notifyTestPlatform) ReconstructReplyCtx(sessionKey string) (any, error) {
	if !p.reconstruct {
		return nil, ErrNotSupported
	}
	p.mu.Lock()
	p.reconstructed = append(p.reconstructed, sessionKey)
	p.mu.Unlock()
	return "reply:" + sessionKey, nil
}

func (p *notifyTestPlatform) SendNotification(_ context.Context, userID, title, content string, metadata map[string]string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.notifications = append(p.notifications, NotifyRequest{
		Platform: p.Name(),
		UserID:   userID,
		Title:    title,
		Content:  content,
		Metadata: cloneStringMap(metadata),
	})
	return p.notificationErr
}

func (p *notifyTestPlatform) notificationCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.notifications)
}

func (p *notifyTestPlatform) lastNotification() NotifyRequest {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.notifications) == 0 {
		return NotifyRequest{}
	}
	return p.notifications[len(p.notifications)-1]
}

type notifyRecordingAgent struct {
	session *notifyRecordingAgentSession
}

func (a *notifyRecordingAgent) Name() string { return "stub" }
func (a *notifyRecordingAgent) StartSession(_ context.Context, _ string) (AgentSession, error) {
	return a.session, nil
}
func (a *notifyRecordingAgent) ListSessions(_ context.Context) ([]AgentSessionInfo, error) {
	return nil, nil
}
func (a *notifyRecordingAgent) Stop() error { return nil }

type notifyRecordingAgentSession struct {
	mu      sync.Mutex
	events  chan Event
	prompts []string
}

func newNotifyRecordingAgentSession() *notifyRecordingAgentSession {
	return &notifyRecordingAgentSession{events: make(chan Event, 10)}
}

func (s *notifyRecordingAgentSession) Send(prompt string, _ []ImageAttachment, _ []FileAttachment) error {
	s.mu.Lock()
	s.prompts = append(s.prompts, prompt)
	s.mu.Unlock()
	s.events <- Event{Type: EventResult, Content: "summary", Done: true}
	return nil
}

func (s *notifyRecordingAgentSession) RespondPermission(_ string, _ PermissionResult) error {
	return nil
}
func (s *notifyRecordingAgentSession) Events() <-chan Event     { return s.events }
func (s *notifyRecordingAgentSession) CurrentSessionID() string { return "notify-session" }
func (s *notifyRecordingAgentSession) Alive() bool              { return true }
func (s *notifyRecordingAgentSession) Close() error             { return nil }

func (s *notifyRecordingAgentSession) promptCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.prompts)
}

func (s *notifyRecordingAgentSession) lastPrompt() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.prompts) == 0 {
		return ""
	}
	return s.prompts[len(s.prompts)-1]
}

func waitFor(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}

func newNotifyTestAPI(platform *notifyTestPlatform, agentSession *notifyRecordingAgentSession) (*APIServer, *Engine) {
	agent := &notifyRecordingAgent{session: agentSession}
	engine := NewEngine("test", agent, []Platform{platform}, "", LangEnglish)
	return &APIServer{engines: map[string]*Engine{"test": engine}}, engine
}

func postNotifySession(t *testing.T, api *APIServer, reqBody NotifyRequest) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(reqBody)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/notify-session", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	api.handleNotifySession(rec, req)
	return rec
}

func TestStripStatusPrefix(t *testing.T) {
	tests := map[string]string{
		"":                  "",
		"  ":                "",
		"status:code-review": "code-review",
		"Status:Code-Review": "Code-Review",
		"STATUS:Blocked":    "Blocked",
		"area:backend":      "area:backend", // no status prefix -> unchanged
		"status:":           "",             // empty after prefix
		"statusoid:foo":     "statusoid:foo",
	}
	for in, want := range tests {
		if got := stripStatusPrefix(in); got != want {
			t.Errorf("stripStatusPrefix(%q) = %q; want %q", in, got, want)
		}
	}
}

func TestHumanizeIssueElapsed(t *testing.T) {
	now := time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		in   string
		want string
	}{
		{"", ""},
		{"not-a-date", ""},
		{"2026-05-28T11:59:30Z", "刚刚"},                // 30s
		{"2026-05-28T11:45:00Z", "15 分钟"},             // 15min
		{"2026-05-28T08:30:00Z", "3 小时 30 分钟"},        // 3h30m
		{"2026-05-28T11:00:00Z", "1 小时"},              // exactly 1h
		{"2026-05-26T11:00:00Z", "2 天 1 小时"},          // 2d1h
		{"2026-05-26T12:00:00Z", "2 天"},               // exactly 2d
		{"2026-05-28T12:00:30Z", "刚刚"},                // future -> 刚刚
	}
	for _, tt := range tests {
		if got := humanizeIssueElapsed(tt.in, now); got != tt.want {
			t.Errorf("humanizeIssueElapsed(%q) = %q; want %q", tt.in, got, tt.want)
		}
	}
}

func TestStaffIDFromAlibabaUserID(t *testing.T) {
	tests := []struct {
		in   string
		want string
		ok   bool
	}{
		{"1001@alibaba-inc.com", "1001", true},
		{" 1001@alibaba-inc.com ", "1001", true},
		{"1001@ALIBABA-INC.COM", "1001", true},
		{"1001", "", false},
		{"1001@example.com", "", false},
		{"@alibaba-inc.com", "", false},
		{"", "", false},
	}
	for _, tt := range tests {
		got, ok := staffIDFromAlibabaUserID(tt.in)
		if got != tt.want || ok != tt.ok {
			t.Fatalf("staffIDFromAlibabaUserID(%q) = %q, %v; want %q, %v", tt.in, got, ok, tt.want, tt.ok)
		}
	}
}

func TestFindActiveDirectSessionByUserID(t *testing.T) {
	sm := NewSessionManager("")
	keys := []string{
		"dingtalk:d:conv1:1001",
		"dingtalk:d:conv2:1001",
		"dingtalk:g:group:1001",
		"dingtalk:d:conv3:2002",
		"feishu:d:conv4:1001",
	}
	for _, key := range keys {
		sm.GetOrCreateActive(key)
		time.Sleep(time.Millisecond)
	}

	if got := sm.FindActiveDirectSessionByUserID("dingtalk", "1001"); got != "dingtalk:d:conv2:1001" {
		t.Fatalf("session = %q, want latest direct dingtalk session", got)
	}
	if got := sm.FindActiveDirectSessionByUserID("dingtalk", "2002"); got != "dingtalk:d:conv3:2002" {
		t.Fatalf("session = %q, want dingtalk:d:conv3:2002", got)
	}
	if got := sm.FindActiveDirectSessionByUserID("dingtalk", "9999"); got != "" {
		t.Fatalf("missing user session = %q, want empty", got)
	}
}

func TestHandleNotifySession_DisabledFallsBackToDirectNotify(t *testing.T) {
	platform := &notifyTestPlatform{stubPlatformEngine: stubPlatformEngine{n: "dingtalk"}, reconstruct: true}
	api, _ := newNotifyTestAPI(platform, newNotifyRecordingAgentSession())
	api.SetNotifySessionSummaryConfig(NotifySessionSummaryConfig{Enabled: false})

	rec := postNotifySession(t, api, NotifyRequest{
		Platform: "dingtalk",
		UserID:   "1001",
		Title:    "Issue updated",
		Content:  "body",
		Metadata: map[string]string{"issue_id": "issue-1"},
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	if platform.notificationCount() != 1 {
		t.Fatalf("notifications = %d, want 1", platform.notificationCount())
	}
	got := platform.lastNotification()
	if got.UserID != "1001" || got.Title != "Issue updated" || got.Content != "body" || got.Metadata["issue_id"] != "issue-1" {
		t.Fatalf("notification = %#v", got)
	}
}

func TestHandleNotifySession_EnabledNoSessionFallsBackToDirectNotify(t *testing.T) {
	platform := &notifyTestPlatform{stubPlatformEngine: stubPlatformEngine{n: "dingtalk"}, reconstruct: true}
	api, _ := newNotifyTestAPI(platform, newNotifyRecordingAgentSession())
	api.SetNotifySessionSummaryConfig(NotifySessionSummaryConfig{Enabled: true, IdleWait: 10 * time.Millisecond, MaxWait: 50 * time.Millisecond})

	rec := postNotifySession(t, api, NotifyRequest{
		Platform: "dingtalk",
		UserID:   "1001@alibaba-inc.com",
		Title:    "Issue updated",
		Content:  "body",
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	if platform.notificationCount() != 1 {
		t.Fatalf("notifications = %d, want 1", platform.notificationCount())
	}
	if got := platform.lastNotification().UserID; got != "1001" {
		t.Fatalf("fallback userID = %q, want stripped staff id", got)
	}
}

func TestHandleNotifySession_QueuesAndInjectsCombinedPrompt(t *testing.T) {
	platform := &notifyTestPlatform{stubPlatformEngine: stubPlatformEngine{n: "dingtalk"}, reconstruct: true}
	agentSession := newNotifyRecordingAgentSession()
	api, engine := newNotifyTestAPI(platform, agentSession)
	sessionKey := "dingtalk:d:conv1:1001"
	engine.sessions.GetOrCreateActive(sessionKey)
	api.SetNotifySessionSummaryConfig(NotifySessionSummaryConfig{
		Enabled:       true,
		Template:      `staff={{.staffId}} issue={{.issueId}} status={{.issueStatus}} count={{.notificationCount}} len={{.summaryLength}} {{.combinedContent}}`,
		SummaryLength: 120,
		IdleWait:      20 * time.Millisecond,
		MaxWait:       200 * time.Millisecond,
	})

	first := postNotifySession(t, api, NotifyRequest{
		Platform: "dingtalk",
		UserID:   "1001@alibaba-inc.com",
		Title:    "First",
		Content:  "first body",
		Metadata: map[string]string{
			"workspace_id": "ws-1",
			"issue_id":     "issue-1",
			"issue_status": "todo",
		},
	})
	if first.Code != http.StatusOK {
		t.Fatalf("first status = %d, body=%s", first.Code, first.Body.String())
	}
	second := postNotifySession(t, api, NotifyRequest{
		Platform: "dingtalk",
		UserID:   "1001@alibaba-inc.com",
		Title:    "Second",
		Content:  "second body",
		Metadata: map[string]string{
			"issue_id":     "issue-1",
			"issue_status": "in_progress",
		},
	})
	if second.Code != http.StatusOK {
		t.Fatalf("second status = %d, body=%s", second.Code, second.Body.String())
	}

	waitFor(t, time.Second, func() bool { return agentSession.promptCount() == 1 })
	if platform.notificationCount() != 0 {
		t.Fatalf("direct notifications = %d, want 0", platform.notificationCount())
	}
	prompt := agentSession.lastPrompt()
	for _, want := range []string{"staff=1001", "issue=issue-1", "status=in_progress", "count=2", "len=120", "First", "first body", "Second", "second body"} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("prompt missing %q:\n%s", want, prompt)
		}
	}
}

func TestHandleNotifySession_InjectFailureFallsBackToOriginalNotifications(t *testing.T) {
	platform := &notifyTestPlatform{stubPlatformEngine: stubPlatformEngine{n: "dingtalk"}}
	api, engine := newNotifyTestAPI(platform, newNotifyRecordingAgentSession())
	engine.sessions.GetOrCreateActive("dingtalk:d:conv1:1001")
	api.SetNotifySessionSummaryConfig(NotifySessionSummaryConfig{
		Enabled:  true,
		IdleWait: 10 * time.Millisecond,
		MaxWait:  50 * time.Millisecond,
	})

	rec := postNotifySession(t, api, NotifyRequest{
		Platform: "dingtalk",
		UserID:   "1001@alibaba-inc.com",
		Title:    "Issue updated",
		Content:  "body",
		Metadata: map[string]string{"issue_id": "issue-1"},
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	waitFor(t, time.Second, func() bool { return platform.notificationCount() == 1 })
	got := platform.lastNotification()
	if got.UserID != "1001" || got.Title != "Issue updated" || got.Content != "body" || got.Metadata["issue_id"] != "issue-1" {
		t.Fatalf("fallback notification = %#v", got)
	}
}

func TestHandleNotifySession_DirectSendErrorPropagates(t *testing.T) {
	platform := &notifyTestPlatform{
		stubPlatformEngine: stubPlatformEngine{n: "dingtalk"},
		notificationErr:    errors.New("token expired"),
	}
	api, _ := newNotifyTestAPI(platform, newNotifyRecordingAgentSession())

	rec := postNotifySession(t, api, NotifyRequest{
		Platform: "dingtalk",
		UserID:   "not-alibaba@example.com",
		Content:  "body",
	})
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
}
