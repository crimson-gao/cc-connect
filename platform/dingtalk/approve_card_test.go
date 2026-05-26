package dingtalk

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/chenhg5/cc-connect/core"

	"github.com/open-dingtalk/dingtalk-stream-sdk-go/card"
)

func TestApprovalActionToResponseText(t *testing.T) {
	cases := []struct {
		action   string
		want     string
		recognized bool
	}{
		{approvalActionAgree, "allow", true},
		{approvalActionAllAgree, "allow all", true},
		{approvalActionReject, "deny", true},
		{"", "", false},
		{"unknown", "", false},
	}
	for _, c := range cases {
		got, ok := approvalActionToResponseText(c.action)
		if ok != c.recognized {
			t.Errorf("action=%q recognized=%v, want %v", c.action, ok, c.recognized)
		}
		if got != c.want {
			t.Errorf("action=%q got %q, want %q", c.action, got, c.want)
		}
	}
}

func TestContainsPermButtons(t *testing.T) {
	allowDeny := [][]core.ButtonOption{
		{
			{Text: "Allow", Data: permButtonAllow},
			{Text: "Deny", Data: permButtonDeny},
		},
		{
			{Text: "Allow All", Data: permButtonAllowAll},
		},
	}
	if !containsPermButtons(allowDeny) {
		t.Error("expected permission button set to be recognized")
	}

	askq := [][]core.ButtonOption{
		{{Text: "yes", Data: "askq:0:1"}, {Text: "no", Data: "askq:0:2"}},
	}
	if containsPermButtons(askq) {
		t.Error("askq buttons should not be recognized as permission buttons")
	}

	empty := [][]core.ButtonOption{}
	if containsPermButtons(empty) {
		t.Error("empty button rows should not match")
	}
}

func TestSplitApprovalPrompt(t *testing.T) {
	cases := []struct {
		name  string
		in    string
		title string
		body  string
	}{
		{"empty", "", "Permission request", ""},
		{"single line", "Run command: ls -la", "Run command: ls -la", ""},
		{"markdown heading + body", "# Tool: Bash\n```bash\nls\n```", "Tool: Bash", "```bash\nls\n```"},
		{"blockquote prefix", "> Tool: Edit\nfile.txt content", "Tool: Edit", "file.txt content"},
		{"only newlines around", "\n\nApprove?\n\nDetails go here.\n", "Approve?", "Details go here."},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			title, body := splitApprovalPrompt(c.in)
			if title != c.title {
				t.Errorf("title = %q, want %q", title, c.title)
			}
			if body != c.body {
				t.Errorf("body = %q, want %q", body, c.body)
			}
		})
	}
}

func TestBuildApprovalCardParams(t *testing.T) {
	got := buildApprovalCardParams("Run ls", "command: `ls`", "2026-05-26 10:00:00")
	want := map[string]string{
		"title":      "Run ls",
		"message":    "command: `ls`",
		"createTime": "2026-05-26 10:00:00",
		"status":     "",
		"summary":    "Run ls",
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("key=%q got=%q want=%q", k, got[k], v)
		}
	}

	// errorMessage is private — must NOT appear in the public cardParamMap
	// at creation time. The callback handler sets it via UserPrivateData.
	if _, present := got["errorMessage"]; present {
		t.Errorf("errorMessage should not be in public cardParamMap, got %+v", got)
	}

	// Empty title falls back to a friendly summary.
	got = buildApprovalCardParams("", "", "")
	if got["summary"] == "" {
		t.Error("summary should default to a non-empty placeholder when title is empty")
	}
}

func TestApprovalSuccessResponse(t *testing.T) {
	resp := approvalSuccessResponse(approvalActionAgree)
	if resp == nil || resp.CardData == nil || resp.UserPrivateData == nil {
		t.Fatalf("expected non-nil response with CardData and UserPrivateData")
	}
	// status is PUBLIC: must be the action so the schema flips to the
	// matching disabled button.
	if got := resp.CardData.CardParamMap["status"]; got != approvalActionAgree {
		t.Errorf("public status = %q, want %q", got, approvalActionAgree)
	}
	if _, present := resp.CardData.CardParamMap["errorMessage"]; present {
		t.Error("errorMessage must NOT leak into public CardData on success")
	}
	// errorMessage is PRIVATE: cleared so any stale error vanishes for the
	// clicker. status should not appear in the private map.
	if got, ok := resp.UserPrivateData.CardParamMap["errorMessage"]; !ok || got != "" {
		t.Errorf("private errorMessage = %q (present=%v), want empty string", got, ok)
	}
	if _, present := resp.UserPrivateData.CardParamMap["status"]; present {
		t.Error("status must NOT appear in UserPrivateData (it's public-only)")
	}
	if resp.CardUpdateOptions == nil || !resp.CardUpdateOptions.UpdateCardDataByKey || !resp.CardUpdateOptions.UpdatePrivateDataByKey {
		t.Error("expected UpdateCardDataByKey AND UpdatePrivateDataByKey to be true")
	}
}

func TestApprovalFailureResponse(t *testing.T) {
	resp := approvalFailureResponse("delivery failed")
	if resp == nil || resp.CardData == nil || resp.UserPrivateData == nil {
		t.Fatalf("expected non-nil response with CardData and UserPrivateData")
	}
	// status is PUBLIC and empty so original buttons remain visible for retry.
	if got := resp.CardData.CardParamMap["status"]; got != "" {
		t.Errorf("public status = %q, want empty string for failure", got)
	}
	if _, present := resp.CardData.CardParamMap["errorMessage"]; present {
		t.Error("errorMessage must NOT leak into public CardData on failure")
	}
	// errorMessage is PRIVATE and shows the reason only to the clicker.
	if got := resp.UserPrivateData.CardParamMap["errorMessage"]; got != "delivery failed" {
		t.Errorf("private errorMessage = %q, want %q", got, "delivery failed")
	}
}

func TestPendingApprovalLifecycle(t *testing.T) {
	p := &Platform{
		pendingApprovals: make(map[string]pendingApproval),
	}

	rc := replyContext{
		conversationId: "conv-1",
		senderStaffId:  "staff-1",
		isGroup:        false,
	}
	p.trackPendingApproval("track-1", pendingApproval{
		sessionKey: rc.sessionKey(false),
		replyCtx:   rc,
		createdAt:  time.Now(),
	})

	got, ok := p.lookupPendingApproval("track-1")
	if !ok {
		t.Fatal("expected to find pending approval")
	}
	if got.sessionKey != "dingtalk:d:conv-1:staff-1" {
		t.Errorf("sessionKey = %q, want %q", got.sessionKey, "dingtalk:d:conv-1:staff-1")
	}

	p.clearPendingApproval("track-1")
	if _, ok := p.lookupPendingApproval("track-1"); ok {
		t.Error("expected pending approval to be cleared")
	}
}

func TestPendingApprovalExpiry(t *testing.T) {
	p := &Platform{
		pendingApprovals: map[string]pendingApproval{
			"old": {
				sessionKey: "dingtalk:d:conv-old:staff",
				createdAt:  time.Now().Add(-(approvalRetention + time.Hour)),
			},
		},
	}
	// Triggering a new track should GC the old entry.
	p.trackPendingApproval("fresh", pendingApproval{
		sessionKey: "dingtalk:d:conv-new:staff",
		createdAt:  time.Now(),
	})
	if _, ok := p.lookupPendingApproval("old"); ok {
		t.Error("expected expired approval to be pruned on next write")
	}
	if _, ok := p.lookupPendingApproval("fresh"); !ok {
		t.Error("fresh approval should be present")
	}
}

func TestReplyContextSessionKey(t *testing.T) {
	direct := replyContext{conversationId: "conv-1", senderStaffId: "u-1", isGroup: false}
	if got := direct.sessionKey(false); got != "dingtalk:d:conv-1:u-1" {
		t.Errorf("direct: got %q", got)
	}

	group := replyContext{conversationId: "conv-2", senderStaffId: "u-1", isGroup: true}
	if got := group.sessionKey(false); got != "dingtalk:g:conv-2:u-1" {
		t.Errorf("group: got %q", got)
	}

	// shareSessionInChannel collapses to per-conversation key.
	if got := group.sessionKey(true); got != "dingtalk:g:conv-2" {
		t.Errorf("group shared: got %q", got)
	}
}

// TestOnCardCallbackUnknownAction ensures unknown actions reply with an error
// CardResponse (so the user sees errorMessage) and don't crash.
func TestOnCardCallbackUnknownAction(t *testing.T) {
	p := &Platform{
		pendingApprovals: make(map[string]pendingApproval),
	}

	req := &card.CardRequest{
		OutTrackId: "track-unknown",
		UserId:     "u-1",
	}
	req.CardActionData = card.PrivateCardActionData{
		CardPrivateData: card.CardPrivateData{
			ActionIdList: []string{"weird"},
			Params:       map[string]any{approvalParamName: "totally_wrong"},
		},
	}
	resp, err := p.onCardCallback(context.Background(), req)
	if err != nil {
		t.Fatalf("onCardCallback returned error: %v", err)
	}
	if resp == nil || resp.CardData == nil || resp.UserPrivateData == nil {
		t.Fatal("expected a failure CardResponse with both CardData and UserPrivateData")
	}
	if resp.CardData.CardParamMap["status"] != "" {
		t.Errorf("expected public status=\"\" on unknown action (keeps buttons), got %q",
			resp.CardData.CardParamMap["status"])
	}
	if resp.UserPrivateData.CardParamMap["errorMessage"] == "" {
		t.Error("expected private errorMessage to be populated for unknown action")
	}
}

// TestOnCardCallbackUnknownTrackId ensures a stale/unmatched outTrackId
// surfaces a clear error to the user without panicking.
func TestOnCardCallbackUnknownTrackId(t *testing.T) {
	p := &Platform{
		pendingApprovals: make(map[string]pendingApproval),
	}
	p.handler = func(_ core.Platform, _ *core.Message) {
		t.Fatal("handler should not be called when track id is unknown")
	}

	req := &card.CardRequest{
		OutTrackId: "track-unmatched",
		UserId:     "u-1",
	}
	req.CardActionData = card.PrivateCardActionData{
		CardPrivateData: card.CardPrivateData{
			Params: map[string]any{approvalParamName: approvalActionAgree},
		},
	}
	resp, err := p.onCardCallback(context.Background(), req)
	if err != nil {
		t.Fatalf("onCardCallback returned error: %v", err)
	}
	if resp.CardData.CardParamMap["status"] != "" {
		t.Errorf("expected public status=\"\" for unmatched track id, got %q",
			resp.CardData.CardParamMap["status"])
	}
	if resp.UserPrivateData.CardParamMap["errorMessage"] == "" {
		t.Error("expected private errorMessage for unmatched track id")
	}
}

// TestOnCardCallbackHappyPath verifies the handler forwards the decision to
// the engine with the expected response text and returns the success
// CardResponse for the matching action.
func TestOnCardCallbackHappyPath(t *testing.T) {
	p := &Platform{
		pendingApprovals: make(map[string]pendingApproval),
	}

	rc := replyContext{conversationId: "conv-1", senderStaffId: "staff-1"}
	p.trackPendingApproval("track-ok", pendingApproval{
		sessionKey: rc.sessionKey(false),
		replyCtx:   rc,
		createdAt:  time.Now(),
	})

	var (
		mu          sync.Mutex
		gotMsg      *core.Message
		handlerWait = make(chan struct{})
	)
	p.handler = func(_ core.Platform, m *core.Message) {
		mu.Lock()
		gotMsg = m
		mu.Unlock()
		close(handlerWait)
	}

	req := &card.CardRequest{
		OutTrackId: "track-ok",
		UserId:     "clicker",
	}
	req.CardActionData = card.PrivateCardActionData{
		CardPrivateData: card.CardPrivateData{
			Params: map[string]any{approvalParamName: approvalActionAllAgree},
		},
	}
	resp, err := p.onCardCallback(context.Background(), req)
	if err != nil {
		t.Fatalf("onCardCallback returned error: %v", err)
	}

	select {
	case <-handlerWait:
	case <-time.After(2 * time.Second):
		t.Fatal("handler was not called within timeout")
	}

	mu.Lock()
	defer mu.Unlock()
	if gotMsg == nil {
		t.Fatal("handler did not receive message")
	}
	if gotMsg.Content != "allow all" {
		t.Errorf("forwarded content = %q, want %q", gotMsg.Content, "allow all")
	}
	if gotMsg.SessionKey != "dingtalk:d:conv-1:staff-1" {
		t.Errorf("forwarded sessionKey = %q", gotMsg.SessionKey)
	}
	if resp == nil || resp.CardData.CardParamMap["status"] != approvalActionAllAgree {
		t.Errorf("CardResponse status = %q, want %q",
			resp.CardData.CardParamMap["status"], approvalActionAllAgree)
	}
	// The entry must remain in the map (with resolvedAction set) so a
	// duplicate click can be handled idempotently.
	got, ok := p.lookupPendingApproval("track-ok")
	if !ok {
		t.Fatal("pending approval should be retained after a successful click")
	}
	if got.resolvedAction != approvalActionAllAgree {
		t.Errorf("resolvedAction = %q, want %q", got.resolvedAction, approvalActionAllAgree)
	}
	if got.resolvedAt.IsZero() {
		t.Error("resolvedAt should be set after a successful click")
	}
}

// TestOnCardCallbackIdempotent verifies that a second click on an
// already-resolved card returns the original action without re-dispatching
// the synthetic message to the engine.
func TestOnCardCallbackIdempotent(t *testing.T) {
	p := &Platform{
		pendingApprovals: make(map[string]pendingApproval),
	}

	rc := replyContext{conversationId: "conv-idem", senderStaffId: "staff-idem"}
	p.trackPendingApproval("track-idem", pendingApproval{
		sessionKey: rc.sessionKey(false),
		replyCtx:   rc,
		createdAt:  time.Now(),
	})

	var handlerCalls int32
	var handlerMu sync.Mutex
	var firstCallDone = make(chan struct{})
	p.handler = func(_ core.Platform, _ *core.Message) {
		handlerMu.Lock()
		handlerCalls++
		v := handlerCalls
		handlerMu.Unlock()
		if v == 1 {
			close(firstCallDone)
		}
	}

	makeReq := func(action string) *card.CardRequest {
		r := &card.CardRequest{OutTrackId: "track-idem", UserId: "clicker"}
		r.CardActionData = card.PrivateCardActionData{
			CardPrivateData: card.CardPrivateData{
				Params: map[string]any{approvalParamName: action},
			},
		}
		return r
	}

	// First click: agree → process normally.
	resp, err := p.onCardCallback(context.Background(), makeReq(approvalActionAgree))
	if err != nil {
		t.Fatalf("first callback error: %v", err)
	}
	if resp.CardData.CardParamMap["status"] != approvalActionAgree {
		t.Errorf("first response status = %q, want %q",
			resp.CardData.CardParamMap["status"], approvalActionAgree)
	}

	select {
	case <-firstCallDone:
	case <-time.After(2 * time.Second):
		t.Fatal("first handler invocation timed out")
	}

	// Second click: even with a different action, the response must echo
	// the original resolved action and NOT trigger another handler call.
	resp2, err := p.onCardCallback(context.Background(), makeReq(approvalActionReject))
	if err != nil {
		t.Fatalf("second callback error: %v", err)
	}
	if resp2.CardData.CardParamMap["status"] != approvalActionAgree {
		t.Errorf("idempotent response status = %q, want original %q",
			resp2.CardData.CardParamMap["status"], approvalActionAgree)
	}

	// Wait a short window to confirm no extra handler dispatch.
	time.Sleep(50 * time.Millisecond)
	handlerMu.Lock()
	defer handlerMu.Unlock()
	if handlerCalls != 1 {
		t.Errorf("handler invocations = %d, want 1 (idempotent re-click must not re-dispatch)", handlerCalls)
	}
}

// TestSendWithButtonsNonPermFallback ensures non-permission buttons fall
// back to ErrNotSupported so the engine routes through the text path.
func TestSendWithButtonsNonPermFallback(t *testing.T) {
	p := &Platform{
		approvalCardTemplateID: defaultApprovalCardTemplateID,
		pendingApprovals:       make(map[string]pendingApproval),
	}

	buttons := [][]core.ButtonOption{
		{{Text: "Yes", Data: "askq:0:1"}, {Text: "No", Data: "askq:0:2"}},
	}
	err := p.SendWithButtons(context.Background(), replyContext{}, "Pick one", buttons)
	if err != core.ErrNotSupported {
		t.Errorf("expected ErrNotSupported for non-perm buttons, got %v", err)
	}
}

// TestSendWithButtonsMissingTemplate ensures the platform falls back to text
// when approval card is not configured (empty template id).
func TestSendWithButtonsMissingTemplate(t *testing.T) {
	p := &Platform{
		approvalCardTemplateID: "", // not configured
		pendingApprovals:       make(map[string]pendingApproval),
	}
	buttons := [][]core.ButtonOption{
		{{Text: "Allow", Data: permButtonAllow}, {Text: "Deny", Data: permButtonDeny}},
	}
	err := p.SendWithButtons(context.Background(), replyContext{}, "Approve?", buttons)
	if err != core.ErrNotSupported {
		t.Errorf("expected ErrNotSupported when template id missing, got %v", err)
	}
}

// TestPlatformImplementsInlineButtonSender ensures the assertion holds.
func TestPlatformImplementsInlineButtonSender(t *testing.T) {
	var _ core.InlineButtonSender = (*Platform)(nil)
}
