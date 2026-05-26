package dingtalk

import (
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

// ──────────────────────────────────────────────────────────────
// Thread safety tests for token caching
// ──────────────────────────────────────────────────────────────

func TestGetAccessToken_ConcurrentAccess(t *testing.T) {
	// This test verifies that concurrent calls to getAccessToken
	// with a pre-cached token are properly synchronized by the mutex

	p := &Platform{
		clientID:     "test_client",
		clientSecret: "test_secret",
		httpClient:   &http.Client{}, // Valid HTTP client
		accessToken:  "test_token",   // Pre-cache a token
		tokenExpiry:  time.Now().Add(1 * time.Hour),
	}

	// Launch multiple goroutines to stress-test the mutex
	const numGoroutines = 100
	var wg sync.WaitGroup
	successCount := 0
	var countMu sync.Mutex

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			token, err := p.getAccessToken()
			if err == nil && token == "test_token" {
				countMu.Lock()
				successCount++
				countMu.Unlock()
			}
		}()
	}

	wg.Wait()

	// All goroutines should have gotten the cached token
	if successCount != numGoroutines {
		t.Errorf("expected %d successful token retrievals, got %d", numGoroutines, successCount)
	}

	t.Logf("Completed %d concurrent token requests without deadlock", numGoroutines)
}

func TestGetAccessToken_MutexExists(t *testing.T) {
	// Verify that the tokenMu mutex field exists and works
	p := &Platform{
		clientID:     "test_client",
		clientSecret: "test_secret",
	}

	// Test that we can lock/unlock the mutex (verify no panic under lock)
	p.tokenMu.Lock()
	_ = p.clientID // SA2001: intentional empty section to verify Lock/Unlock work
	p.tokenMu.Unlock()

	// Test with defer
	p.tokenMu.Lock()
	defer p.tokenMu.Unlock()

	t.Log("tokenMu mutex is functional")
}

func TestGetAccessToken_CachedTokenAccess(t *testing.T) {
	// Test that cached token access is thread-safe
	p := &Platform{
		clientID:     "test_client",
		clientSecret: "test_secret",
		accessToken:  "cached_token",
		tokenExpiry:  time.Now().Add(1 * time.Hour),
	}

	const numGoroutines = 50
	var wg sync.WaitGroup
	tokens := make([]string, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			token, err := p.getAccessToken()
			if err == nil {
				tokens[idx] = token
			}
		}(i)
	}

	wg.Wait()

	// Verify all goroutines got the same cached token
	for i, token := range tokens {
		if token != "" && token != "cached_token" {
			t.Errorf("goroutine %d: expected cached token 'cached_token', got %q", i, token)
		}
	}

	t.Logf("All %d goroutines safely accessed cached token", numGoroutines)
}

func TestPlatform_MutexFieldExists(t *testing.T) {
	// Verify the Platform struct has the tokenMu field
	p := &Platform{}

	// Verify no panic under lock (test will fail to compile if tokenMu doesn't exist)
	p.tokenMu.Lock()
	_ = p.clientID // SA2001: intentional empty section to verify Lock/Unlock work
	p.tokenMu.Unlock()

	t.Log("Platform.tokenMu field exists")
}

func TestPlatform_AccessTokenFieldsExist(t *testing.T) {
	// Verify the Platform struct has the token caching fields
	p := &Platform{}

	// Set the fields
	p.accessToken = "test_token"
	p.tokenExpiry = time.Now().Add(1 * time.Hour)

	// Verify they're set
	if p.accessToken != "test_token" {
		t.Errorf("expected accessToken 'test_token', got %q", p.accessToken)
	}

	t.Log("Platform token caching fields exist and are accessible")
}

// ──────────────────────────────────────────────────────────────
// ReconstructReplyCtx tests
// ──────────────────────────────────────────────────────────────

func TestReconstructReplyCtx_GroupSharedSession(t *testing.T) {
	p := &Platform{}
	rctx, err := p.ReconstructReplyCtx("dingtalk:g:conv123")
	if err != nil {
		t.Fatalf("ReconstructReplyCtx() error = %v", err)
	}
	rc := rctx.(replyContext)
	if rc.conversationId != "conv123" {
		t.Errorf("conversationId = %q, want %q", rc.conversationId, "conv123")
	}
	if rc.senderStaffId != "" {
		t.Errorf("senderStaffId = %q, want empty", rc.senderStaffId)
	}
	if !rc.isGroup {
		t.Error("isGroup = false, want true for group session")
	}
	if !rc.proactive {
		t.Error("proactive = false, want true")
	}
}

func TestReconstructReplyCtx_GroupPerUserSession(t *testing.T) {
	p := &Platform{}
	rctx, err := p.ReconstructReplyCtx("dingtalk:g:conv123:user456")
	if err != nil {
		t.Fatalf("ReconstructReplyCtx() error = %v", err)
	}
	rc := rctx.(replyContext)
	if rc.conversationId != "conv123" {
		t.Errorf("conversationId = %q, want %q", rc.conversationId, "conv123")
	}
	if rc.senderStaffId != "user456" {
		t.Errorf("senderStaffId = %q, want %q", rc.senderStaffId, "user456")
	}
	if !rc.isGroup {
		t.Error("isGroup = false, want true for group session")
	}
}

func TestReconstructReplyCtx_DirectSession(t *testing.T) {
	p := &Platform{}
	rctx, err := p.ReconstructReplyCtx("dingtalk:d:conv789:user111")
	if err != nil {
		t.Fatalf("ReconstructReplyCtx() error = %v", err)
	}
	rc := rctx.(replyContext)
	if rc.conversationId != "conv789" {
		t.Errorf("conversationId = %q, want %q", rc.conversationId, "conv789")
	}
	if rc.senderStaffId != "user111" {
		t.Errorf("senderStaffId = %q, want %q", rc.senderStaffId, "user111")
	}
	if rc.isGroup {
		t.Error("isGroup = true, want false for direct session")
	}
	if !rc.proactive {
		t.Error("proactive = false, want true")
	}
}

func TestReconstructReplyCtx_InvalidPrefix(t *testing.T) {
	p := &Platform{}
	_, err := p.ReconstructReplyCtx("telegram:g:conv123")
	if err == nil {
		t.Fatal("expected error for non-dingtalk prefix")
	}
}

func TestReconstructReplyCtx_InvalidConvType(t *testing.T) {
	p := &Platform{}
	_, err := p.ReconstructReplyCtx("dingtalk:x:conv123")
	if err == nil {
		t.Fatal("expected error for invalid conversation type")
	}
}

func TestReconstructReplyCtx_EmptyConversationId(t *testing.T) {
	p := &Platform{}
	_, err := p.ReconstructReplyCtx("dingtalk:g:")
	if err == nil {
		t.Fatal("expected error for empty conversationId")
	}
}

func TestReconstructReplyCtx_TooFewParts(t *testing.T) {
	p := &Platform{}
	_, err := p.ReconstructReplyCtx("dingtalk:")
	if err == nil {
		t.Fatal("expected error for too few parts")
	}
}

// ──────────────────────────────────────────────────────────────
// formatReplyContent tests
// ──────────────────────────────────────────────────────────────

func TestFormatReplyContent_WithQuotedText(t *testing.T) {
	p := &Platform{}
	repliedContent, _ := json.Marshal(repliedTextContent{Text: "original message"})
	richText := &richTextContent{
		Content:    "user reply",
		IsReplyMsg: true,
		RepliedMsg: &repliedMessage{
			MsgType: "text",
			Content: repliedContent,
		},
	}
	result := p.formatReplyContent(richText, "fallback", "")
	expected := "引用: \"original message\"\n\nuser reply"
	if result != expected {
		t.Errorf("formatReplyContent() = %q, want %q", result, expected)
	}
}

func TestFormatReplyContent_EmptyContent_UsesFallback(t *testing.T) {
	p := &Platform{}
	repliedContent, _ := json.Marshal(repliedTextContent{Text: "quoted"})
	richText := &richTextContent{
		Content:    "",
		IsReplyMsg: true,
		RepliedMsg: &repliedMessage{
			MsgType: "text",
			Content: repliedContent,
		},
	}
	result := p.formatReplyContent(richText, "fallback text", "")
	expected := "引用: \"quoted\"\n\nfallback text"
	if result != expected {
		t.Errorf("formatReplyContent() = %q, want %q", result, expected)
	}
}

func TestFormatReplyContent_NilRepliedMsg(t *testing.T) {
	p := &Platform{}
	richText := &richTextContent{
		Content:    "just a message",
		IsReplyMsg: true,
		RepliedMsg: nil,
	}
	result := p.formatReplyContent(richText, "fallback", "")
	if result != "just a message" {
		t.Errorf("formatReplyContent() = %q, want %q", result, "just a message")
	}
}

func TestFormatReplyContent_NonTextMsgType(t *testing.T) {
	p := &Platform{}
	richText := &richTextContent{
		Content:    "user reply",
		IsReplyMsg: true,
		RepliedMsg: &repliedMessage{
			MsgType: "image",
			Content: json.RawMessage(`{}`),
		},
	}
	result := p.formatReplyContent(richText, "fallback", "")
	if result != "user reply" {
		t.Errorf("formatReplyContent() = %q, want %q", result, "user reply")
	}
}

func TestFormatReplyContent_MarkdownQuotedMessage(t *testing.T) {
	p := &Platform{}
	repliedContent, _ := json.Marshal(repliedMarkdownContent{
		Title: "Multica · Fix login bug",
		Text:  "### Multica · Fix login bug\n\nSome body text",
	})
	richText := &richTextContent{
		Content:    "user reply",
		IsReplyMsg: true,
		RepliedMsg: &repliedMessage{
			MsgType: "sampleMarkdown",
			Content: repliedContent,
		},
	}
	result := p.formatReplyContent(richText, "fallback", "")
	expected := "引用: \"### Multica · Fix login bug\n\nSome body text\"\n\nuser reply"
	if result != expected {
		t.Errorf("formatReplyContent() = %q, want %q", result, expected)
	}
}

func TestFormatReplyContent_MulticaContextInTextReply(t *testing.T) {
	p := &Platform{}
	quotedText := "### Multica · Fix login\n\n---\n\n> Reply to interact\n\n[multica:ws=aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee,issue=11111111-2222-3333-4444-555555555555]"
	repliedContent, _ := json.Marshal(repliedTextContent{Text: quotedText})
	richText := &richTextContent{
		Content:    "请把这个issue标记为done",
		IsReplyMsg: true,
		RepliedMsg: &repliedMessage{
			MsgType: "text",
			Content: repliedContent,
		},
	}
	result := p.formatReplyContent(richText, "", "")
	if !strings.Contains(result, "[multica-reply workspace_id=aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee issue_id=11111111-2222-3333-4444-555555555555]") {
		t.Errorf("expected multica-reply context header, got:\n%s", result)
	}
	if !strings.Contains(result, "请把这个issue标记为done") {
		t.Errorf("expected user message preserved, got:\n%s", result)
	}
	if !strings.Contains(result, "multica issue") {
		t.Errorf("expected multica CLI examples, got:\n%s", result)
	}
}

func TestFormatReplyContent_MulticaContextInMarkdownReply(t *testing.T) {
	p := &Platform{}
	mdText := "### Multica · Deploy service\n\n---\n\n> Reply to interact\n\n[multica:ws=aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee,issue=11111111-2222-3333-4444-555555555555]"
	repliedContent, _ := json.Marshal(repliedMarkdownContent{
		Title: "Multica · Deploy service",
		Text:  mdText,
	})
	richText := &richTextContent{
		Content:    "add a comment: looks good",
		IsReplyMsg: true,
		RepliedMsg: &repliedMessage{
			MsgType: "sampleMarkdown",
			Content: repliedContent,
		},
	}
	result := p.formatReplyContent(richText, "", "")
	if !strings.Contains(result, "[multica-reply workspace_id=aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee") {
		t.Errorf("expected multica-reply context, got:\n%s", result)
	}
	if !strings.Contains(result, "add a comment: looks good") {
		t.Errorf("expected user message, got:\n%s", result)
	}
}

func TestFormatReplyContent_EmptyQuotedText(t *testing.T) {
	p := &Platform{}
	repliedContent, _ := json.Marshal(repliedTextContent{Text: ""})
	richText := &richTextContent{
		Content:    "user reply",
		IsReplyMsg: true,
		RepliedMsg: &repliedMessage{
			MsgType: "text",
			Content: repliedContent,
		},
	}
	result := p.formatReplyContent(richText, "fallback", "")
	if result != "user reply" {
		t.Errorf("formatReplyContent() = %q, want %q", result, "user reply")
	}
}

func TestFormatReplyContent_NonEmptyQuoteDoesNotUseStoredContextFallback(t *testing.T) {
	p := &Platform{}
	p.storeNotifyContext("user123", map[string]string{
		"workspace_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
		"issue_id":     "11111111-2222-3333-4444-555555555555",
	})
	repliedContent, _ := json.Marshal(repliedTextContent{Text: "### Multica · Fix lo..."})
	richText := &richTextContent{
		Content:    "mark this as done",
		IsReplyMsg: true,
		RepliedMsg: &repliedMessage{
			MsgType: "text",
			Content: repliedContent,
		},
	}
	result := p.formatReplyContent(richText, "", "user123")
	expected := "引用: \"### Multica · Fix lo...\"\n\nmark this as done"
	if result != expected {
		t.Errorf("formatReplyContent() = %q, want %q", result, expected)
	}
}

func TestFormatReplyContent_StoredContextFallbackWithEmptyQuote(t *testing.T) {
	p := &Platform{}
	p.storeNotifyContext("user123", map[string]string{
		"workspace_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
		"issue_id":     "11111111-2222-3333-4444-555555555555",
	})
	repliedContent, _ := json.Marshal(repliedTextContent{Text: ""})
	richText := &richTextContent{
		Content:    "mark this as done",
		IsReplyMsg: true,
		RepliedMsg: &repliedMessage{
			MsgType: "text",
			Content: repliedContent,
		},
	}
	result := p.formatReplyContent(richText, "", "user123")
	if !strings.Contains(result, "[multica-reply workspace_id=aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee issue_id=11111111-2222-3333-4444-555555555555]") {
		t.Errorf("expected stored context fallback, got:\n%s", result)
	}
	if !strings.Contains(result, "mark this as done") {
		t.Errorf("expected user message, got:\n%s", result)
	}
}

func TestFormatReplyContent_GroupDoesNotUseStoredContextFallback(t *testing.T) {
	p := &Platform{}
	p.StoreProactiveContext("dingtalk:g:conv123", map[string]string{
		"workspace_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
		"issue_id":     "11111111-2222-3333-4444-555555555555",
	})
	repliedContent, _ := json.Marshal(repliedTextContent{Text: ""})
	richText := &richTextContent{
		Content:    "mark this as done",
		IsReplyMsg: true,
		RepliedMsg: &repliedMessage{
			MsgType: "text",
			Content: repliedContent,
		},
	}
	result := p.formatReplyContent(richText, "", "user123", "conv123")
	if result != "mark this as done" {
		t.Errorf("formatReplyContent() = %q, want %q", result, "mark this as done")
	}
}

func TestFormatReplyContent_QuotedMarkerWinsOverStoredGroupContext(t *testing.T) {
	p := &Platform{}
	p.StoreProactiveContext("dingtalk:g:conv123", map[string]string{
		"workspace_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
		"issue_id":     "11111111-2222-3333-4444-555555555555",
	})
	quotedText := "[multica:ws=bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb,issue=22222222-3333-4444-5555-666666666666]\n### Multica · Referenced issue"
	repliedContent, _ := json.Marshal(repliedMarkdownContent{
		Title: "Multica · Referenced issue",
		Text:  quotedText,
	})
	richText := &richTextContent{
		Content:    "mark this as done",
		IsReplyMsg: true,
		RepliedMsg: &repliedMessage{
			MsgType: "sampleMarkdown",
			Content: repliedContent,
		},
	}
	result := p.formatReplyContent(richText, "", "user123", "conv123")
	if !strings.Contains(result, "[multica-reply workspace_id=bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb issue_id=22222222-3333-4444-5555-666666666666]") {
		t.Errorf("expected quoted marker to win over stored group context, got:\n%s", result)
	}
	if strings.Contains(result, "11111111-2222-3333-4444-555555555555") {
		t.Errorf("unexpected stale group issue context in result:\n%s", result)
	}
}

func TestFormatReplyContent_IgnoresInvalidStoredContext(t *testing.T) {
	p := &Platform{}
	p.storeNotifyContext("user123", map[string]string{
		"workspace_id": "not-a-uuid",
		"issue_id":     "11111111-2222-3333-4444-555555555555",
	})
	repliedContent, _ := json.Marshal(repliedTextContent{Text: "truncated quote"})
	richText := &richTextContent{
		Content:    "user reply",
		IsReplyMsg: true,
		RepliedMsg: &repliedMessage{
			MsgType: "text",
			Content: repliedContent,
		},
	}
	result := p.formatReplyContent(richText, "", "user123")
	expected := "引用: \"truncated quote\"\n\nuser reply"
	if result != expected {
		t.Errorf("formatReplyContent() = %q, want %q", result, expected)
	}
}

func TestFormatReplyContent_NoStoredContextFallback(t *testing.T) {
	p := &Platform{}
	// No stored context for this user
	repliedContent, _ := json.Marshal(repliedTextContent{Text: "some message"})
	richText := &richTextContent{
		Content:    "user reply",
		IsReplyMsg: true,
		RepliedMsg: &repliedMessage{
			MsgType: "text",
			Content: repliedContent,
		},
	}
	result := p.formatReplyContent(richText, "", "unknown_user")
	expected := "引用: \"some message\"\n\nuser reply"
	if result != expected {
		t.Errorf("formatReplyContent() = %q, want %q", result, expected)
	}
}

// ──────────────────────────────────────────────────────────────
// Proactive routing tests
// ──────────────────────────────────────────────────────────────

func TestProactiveRouting_GroupSessionUsesGroupAPI(t *testing.T) {
	// Verify that a group session key produces a replyContext with isGroup=true,
	// which sendProactiveMessage would route to groupMessages/send.
	p := &Platform{}
	rctx, err := p.ReconstructReplyCtx("dingtalk:g:conv123:user456")
	if err != nil {
		t.Fatalf("ReconstructReplyCtx() error = %v", err)
	}
	rc := rctx.(replyContext)
	if !rc.isGroup || rc.conversationId == "" {
		t.Errorf("group routing: isGroup=%v, conversationId=%q; want isGroup=true with non-empty conversationId", rc.isGroup, rc.conversationId)
	}
}

func TestProactiveRouting_DirectSessionUsesDirectAPI(t *testing.T) {
	// Verify that a direct session key produces a replyContext with isGroup=false,
	// which sendProactiveMessage would route to oToMessages/batchSend.
	p := &Platform{}
	rctx, err := p.ReconstructReplyCtx("dingtalk:d:conv789:user111")
	if err != nil {
		t.Fatalf("ReconstructReplyCtx() error = %v", err)
	}
	rc := rctx.(replyContext)
	if rc.isGroup {
		t.Error("direct routing: isGroup=true, want false for 1:1 session")
	}
	if rc.senderStaffId != "user111" {
		t.Errorf("direct routing: senderStaffId=%q, want %q", rc.senderStaffId, "user111")
	}
}

// ──────────────────────────────────────────────────────────────
// extractRichText tests (from main: richText message type support)
// ──────────────────────────────────────────────────────────────

func TestExtractRichText(t *testing.T) {
	tests := []struct {
		name    string
		content interface{}
		want    string
	}{
		{
			name:    "nil content",
			content: nil,
			want:    "",
		},
		{
			name:    "non-map content",
			content: "not a map",
			want:    "",
		},
		{
			name: "empty richText array",
			content: map[string]interface{}{
				"richText": []interface{}{},
			},
			want: "",
		},
		{
			name: "single text element",
			content: map[string]interface{}{
				"richText": []interface{}{
					map[string]interface{}{"text": "Hello World"},
				},
			},
			want: "Hello World",
		},
		{
			name: "multiple text elements",
			content: map[string]interface{}{
				"richText": []interface{}{
					map[string]interface{}{"text": "Hello "},
					map[string]interface{}{"text": "World"},
				},
			},
			want: "Hello World",
		},
		{
			name: "text with attrs (bold etc) — attrs ignored, text extracted",
			content: map[string]interface{}{
				"richText": []interface{}{
					map[string]interface{}{"text": "normal "},
					map[string]interface{}{"text": "bold", "attrs": map[string]interface{}{"bold": true}},
				},
			},
			want: "normal bold",
		},
		{
			name: "mixed text and picture elements — pictures skipped",
			content: map[string]interface{}{
				"richText": []interface{}{
					map[string]interface{}{"text": "See image: "},
					map[string]interface{}{"pictureDownloadCode": "abc123"},
					map[string]interface{}{"text": "done"},
				},
			},
			want: "See image: done",
		},
		{
			name: "missing richText key",
			content: map[string]interface{}{
				"other": "data",
			},
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractRichText(tt.content)
			if got != tt.want {
				t.Errorf("extractRichText() = %q, want %q", got, tt.want)
			}
		})
	}
}

// ──────────────────────────────────────────────────────────────
// parseMulticaContext tests
// ──────────────────────────────────────────────────────────────

func TestParseMulticaContext_Valid(t *testing.T) {
	text := "some text [multica:ws=aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee,issue=11111111-2222-3333-4444-555555555555] more text"
	ctx := parseMulticaContext(text)
	if ctx == nil {
		t.Fatal("expected non-nil context")
	}
	if ctx.WorkspaceID != "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee" {
		t.Errorf("WorkspaceID = %q, want aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", ctx.WorkspaceID)
	}
	if ctx.IssueID != "11111111-2222-3333-4444-555555555555" {
		t.Errorf("IssueID = %q, want 11111111-2222-3333-4444-555555555555", ctx.IssueID)
	}
}

func TestParseMulticaContext_NoMarker(t *testing.T) {
	ctx := parseMulticaContext("just a normal message")
	if ctx != nil {
		t.Error("expected nil context for message without marker")
	}
}

func TestParseMulticaContext_MalformedUUID(t *testing.T) {
	ctx := parseMulticaContext("[multica:ws=not-a-uuid,issue=also-bad]")
	if ctx != nil {
		t.Error("expected nil context for malformed UUIDs")
	}
}

func TestParseMulticaContext_InFullMarkdown(t *testing.T) {
	md := "### Multica · Fix bug\n\n> type: `status_changed`\n\n---\n\n> Reply to interact\n\n[multica:ws=01234567-89ab-cdef-0123-456789abcdef,issue=fedcba98-7654-3210-fedc-ba9876543210]"
	ctx := parseMulticaContext(md)
	if ctx == nil {
		t.Fatal("expected non-nil context from full markdown")
	}
	if ctx.WorkspaceID != "01234567-89ab-cdef-0123-456789abcdef" {
		t.Errorf("WorkspaceID = %q", ctx.WorkspaceID)
	}
	if ctx.IssueID != "fedcba98-7654-3210-fedc-ba9876543210" {
		t.Errorf("IssueID = %q", ctx.IssueID)
	}
}

// ──────────────────────────────────────────────────────────────
// extractQuotedText tests
// ──────────────────────────────────────────────────────────────

func TestExtractQuotedText_TextType(t *testing.T) {
	content, _ := json.Marshal(repliedTextContent{Text: "hello world"})
	got := extractQuotedText(&repliedMessage{MsgType: "text", Content: content})
	if got != "hello world" {
		t.Errorf("extractQuotedText(text) = %q, want %q", got, "hello world")
	}
}

func TestExtractQuotedText_SampleMarkdownType(t *testing.T) {
	content, _ := json.Marshal(repliedMarkdownContent{Title: "Title", Text: "body text"})
	got := extractQuotedText(&repliedMessage{MsgType: "sampleMarkdown", Content: content})
	if got != "body text" {
		t.Errorf("extractQuotedText(sampleMarkdown) = %q, want %q", got, "body text")
	}
}

func TestExtractQuotedText_SampleMarkdownTitlePlaceholder(t *testing.T) {
	content, _ := json.Marshal(repliedMarkdownContent{
		Title: "[multica:ws=aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee,issue=11111111-2222-3333-4444-555555555555] Multica · Fix login",
		Text:  "#title#",
	})
	got := extractQuotedText(&repliedMessage{MsgType: "sampleMarkdown", Content: content})
	if !strings.Contains(got, "issue=11111111-2222-3333-4444-555555555555") {
		t.Errorf("extractQuotedText(sampleMarkdown placeholder) = %q, want title with marker", got)
	}
}

func TestExtractQuotedText_MarkdownType(t *testing.T) {
	content, _ := json.Marshal(repliedMarkdownContent{Title: "T", Text: "md body"})
	got := extractQuotedText(&repliedMessage{MsgType: "markdown", Content: content})
	if got != "md body" {
		t.Errorf("extractQuotedText(markdown) = %q, want %q", got, "md body")
	}
}

func TestExtractQuotedText_UnsupportedType(t *testing.T) {
	got := extractQuotedText(&repliedMessage{MsgType: "image", Content: json.RawMessage(`{}`)})
	if got != "" {
		t.Errorf("extractQuotedText(image) = %q, want empty", got)
	}
}

func TestExtractQuotedText_Nil(t *testing.T) {
	got := extractQuotedText(nil)
	if got != "" {
		t.Errorf("extractQuotedText(nil) = %q, want empty", got)
	}
}

func TestDingTalkMarkdownTitle_PreservesMulticaMarker(t *testing.T) {
	content := "[multica:ws=aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee,issue=11111111-2222-3333-4444-555555555555]\n\n### Multica · [AONE-82229688] Fix login"
	got := dingtalkMarkdownTitle(content)
	if !strings.HasPrefix(got, "[multica:ws=aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee,issue=11111111-2222-3333-4444-555555555555]") {
		t.Errorf("dingtalkMarkdownTitle() = %q, want marker prefix", got)
	}
	if !strings.Contains(got, "Multica · [AONE-82229688] Fix login") {
		t.Errorf("dingtalkMarkdownTitle() = %q, want human title", got)
	}
}

func TestOpenSpaceIDFor(t *testing.T) {
	cases := []struct {
		name string
		rc   replyContext
		want string
	}{
		{
			name: "group uses conversationId",
			rc:   replyContext{isGroup: true, conversationId: "cidABCxyz==", senderStaffId: "u-1"},
			want: "dtv1.card//IM_GROUP.cidABCxyz==",
		},
		{
			name: "direct uses senderStaffId (NOT robotCode)",
			rc:   replyContext{isGroup: false, conversationId: "cid-direct", senderStaffId: "staff-42"},
			want: "dtv1.card//IM_ROBOT.staff-42",
		},
		{
			name: "direct with empty senderStaffId still emits the IM_ROBOT prefix (caller's responsibility to validate)",
			rc:   replyContext{isGroup: false, senderStaffId: ""},
			want: "dtv1.card//IM_ROBOT.",
		},
		{
			name: "group with empty conversationId still emits IM_GROUP prefix",
			rc:   replyContext{isGroup: true, conversationId: ""},
			want: "dtv1.card//IM_GROUP.",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := openSpaceIDFor(c.rc); got != c.want {
				t.Errorf("openSpaceIDFor(%+v) = %q, want %q", c.rc, got, c.want)
			}
		})
	}
}
