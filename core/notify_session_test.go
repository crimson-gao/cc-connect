package core

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// cloneStringMap is the local clone helper used by notifyTestPlatform.
// Same shape as the helper previously living in notify_summary.go.
func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

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

func postNotifySession(t *testing.T, api *APIServer, body NotifySessionRequest) *httptest.ResponseRecorder {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/notify-session", bytes.NewReader(raw))
	rec := httptest.NewRecorder()
	api.handleNotifySession(rec, req)
	return rec
}

func postNotify(t *testing.T, api *APIServer, body NotifyRequest) *httptest.ResponseRecorder {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/notify", bytes.NewReader(raw))
	rec := httptest.NewRecorder()
	api.handleNotify(rec, req)
	return rec
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

func TestHandleNotifySession_InjectsPromptVerbatim(t *testing.T) {
	platform := &notifyTestPlatform{stubPlatformEngine: stubPlatformEngine{n: "dingtalk"}, reconstruct: true}
	agentSession := newNotifyRecordingAgentSession()
	api, engine := newNotifyTestAPI(platform, agentSession)
	engine.sessions.GetOrCreateActive("dingtalk:d:conv1:1001")

	prompt := "已收到 3 条通知，请总结一下。"
	rec := postNotifySession(t, api, NotifySessionRequest{
		Platform: "dingtalk",
		UserID:   "1001@alibaba-inc.com",
		Prompt:   prompt,
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	waitFor(t, time.Second, func() bool { return agentSession.promptCount() == 1 })
	if got := agentSession.lastPrompt(); got != prompt {
		t.Fatalf("prompt = %q; want verbatim %q", got, prompt)
	}
	if platform.notificationCount() != 0 {
		t.Fatalf("direct notifications = %d; should be 0 (notify-session is inject-only)", platform.notificationCount())
	}
}

func TestHandleNotifySession_RejectsBareStaffID(t *testing.T) {
	platform := &notifyTestPlatform{stubPlatformEngine: stubPlatformEngine{n: "dingtalk"}, reconstruct: true}
	api, _ := newNotifyTestAPI(platform, newNotifyRecordingAgentSession())
	rec := postNotifySession(t, api, NotifySessionRequest{UserID: "1001", Prompt: "x"})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestHandleNotifySession_404WhenNoSession(t *testing.T) {
	platform := &notifyTestPlatform{stubPlatformEngine: stubPlatformEngine{n: "dingtalk"}, reconstruct: true}
	api, _ := newNotifyTestAPI(platform, newNotifyRecordingAgentSession())
	rec := postNotifySession(t, api, NotifySessionRequest{UserID: "9999@alibaba-inc.com", Prompt: "x"})
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

func TestHandleNotify_RespectsNotifyUserFalse(t *testing.T) {
	platform := &notifyTestPlatform{stubPlatformEngine: stubPlatformEngine{n: "dingtalk"}}
	api, _ := newNotifyTestAPI(platform, newNotifyRecordingAgentSession())
	notifyFalse := false
	rec := postNotify(t, api, NotifyRequest{
		Platform:   "dingtalk",
		UserID:     "1001",
		Title:      "x",
		Content:    "y",
		NotifyUser: &notifyFalse,
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	if platform.notificationCount() != 0 {
		t.Fatalf("notifications = %d; want 0 when notify_user=false", platform.notificationCount())
	}
}

func TestHandleNotify_DefaultsToNotifyUserTrue(t *testing.T) {
	platform := &notifyTestPlatform{stubPlatformEngine: stubPlatformEngine{n: "dingtalk"}}
	api, _ := newNotifyTestAPI(platform, newNotifyRecordingAgentSession())
	rec := postNotify(t, api, NotifyRequest{
		Platform: "dingtalk",
		UserID:   "1001",
		Title:    "x",
		Content:  "y",
		// NotifyUser omitted → default true
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	if platform.notificationCount() != 1 {
		t.Fatalf("notifications = %d; want 1 when notify_user is absent", platform.notificationCount())
	}
}
