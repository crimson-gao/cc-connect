package core

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHandleSend_AllowsAttachmentOnly(t *testing.T) {
	engine := NewEngine("test", &stubAgent{}, []Platform{&stubMediaPlatform{stubPlatformEngine: stubPlatformEngine{n: "test"}}}, "", LangEnglish)
	engine.interactiveStates["session-1"] = &interactiveState{
		platform: &stubMediaPlatform{stubPlatformEngine: stubPlatformEngine{n: "test"}},
		replyCtx: "reply-ctx",
	}

	api := &APIServer{engines: map[string]*Engine{"test": engine}}
	reqBody := SendRequest{
		Project:    "test",
		SessionKey: "session-1",
		Images: []ImageAttachment{{
			MimeType: "image/png",
			Data:     []byte("img"),
			FileName: "chart.png",
		}},
	}
	body, err := json.Marshal(reqBody)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/send", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	api.handleSend(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
}

type stubMetadataPlatform struct {
	stubMediaPlatform
	sessionKey string
	metadata   map[string]string
}

func (p *stubMetadataPlatform) StoreProactiveContext(sessionKey string, metadata map[string]string) {
	p.sessionKey = sessionKey
	p.metadata = metadata
}

func TestHandleSend_StoresMetadata(t *testing.T) {
	platform := &stubMetadataPlatform{stubMediaPlatform: stubMediaPlatform{stubPlatformEngine: stubPlatformEngine{n: "test"}}}
	engine := NewEngine("test", &stubAgent{}, []Platform{platform}, "", LangEnglish)
	engine.interactiveStates["session-1"] = &interactiveState{
		platform: platform,
		replyCtx: "reply-ctx",
	}

	api := &APIServer{engines: map[string]*Engine{"test": engine}}
	reqBody := SendRequest{
		Project:    "test",
		SessionKey: "session-1",
		Message:    "delivery ready",
		Metadata: map[string]string{
			"workspace_id": "ws-1",
			"issue_id":     "issue-1",
		},
	}
	body, err := json.Marshal(reqBody)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/send", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	api.handleSend(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	if platform.sessionKey != "session-1" {
		t.Fatalf("stored session key = %q", platform.sessionKey)
	}
	if platform.metadata["workspace_id"] != "ws-1" || platform.metadata["issue_id"] != "issue-1" {
		t.Fatalf("stored metadata = %#v", platform.metadata)
	}
}

type stubQuestionCardPlatform struct {
	stubPlatformEngine
	userID   string
	schemaID string
	data     QuestionCardData
	metadata map[string]string
}

func (p *stubQuestionCardPlatform) SendQuestionCard(_ context.Context, userID, schemaID string, data QuestionCardData, metadata map[string]string) error {
	p.userID = userID
	p.schemaID = schemaID
	p.data = data
	p.metadata = metadata
	return nil
}

func TestHandleQuestionCardSendsViaPlatform(t *testing.T) {
	platform := &stubQuestionCardPlatform{stubPlatformEngine: stubPlatformEngine{n: "dingtalk"}}
	engine := NewEngine("test", &stubAgent{}, []Platform{platform}, "", LangEnglish)
	api := &APIServer{engines: map[string]*Engine{"test": engine}}

	reqBody := QuestionCardRequest{
		Platform: "dingtalk",
		UserID:   "123456",
		SchemaID: "schema-1",
		CardData: QuestionCardData{
			WorkspaceID: "ws-1",
			IssueID:     "issue-1",
			IssueTitle:  "Fix login",
			QuestionURL: "https://app.example.com/ws/questions?issueId=issue-1&questionId=q-1",
			QuestionID:  "q-1",
			Question:    "Use which option?",
			Options: []QuestionCardOption{
				{Index: 0, Value: "0", Label: "A"},
			},
		},
		Metadata: map[string]string{"recipient_user_id": "u-1"},
	}
	body, err := json.Marshal(reqBody)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/cards/question", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	api.handleQuestionCard(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	if platform.userID != "123456" || platform.schemaID != reqBody.SchemaID {
		t.Fatalf("user/schema = %q/%q", platform.userID, platform.schemaID)
	}
	if platform.data.IssueTitle != "Fix login" || platform.data.QuestionURL == "" {
		t.Fatalf("card data = %#v", platform.data)
	}
	if platform.metadata["recipient_user_id"] != "u-1" {
		t.Fatalf("metadata = %#v", platform.metadata)
	}
}

func TestHandleQuestionCardAllowsSessionKeyTarget(t *testing.T) {
	platform := &stubQuestionCardPlatform{stubPlatformEngine: stubPlatformEngine{n: "dingtalk"}}
	engine := NewEngine("test", &stubAgent{}, []Platform{platform}, "", LangEnglish)
	api := &APIServer{engines: map[string]*Engine{"test": engine}}

	reqBody := QuestionCardRequest{
		Platform:   "dingtalk",
		SessionKey: "dingtalk:g:conv123",
		SchemaID:   "schema-1",
		CardData: QuestionCardData{
			WorkspaceID: "ws-1",
			QuestionID:  "q-1",
			Question:    "Use which option?",
		},
	}
	body, err := json.Marshal(reqBody)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/cards/question", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	api.handleQuestionCard(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	if platform.userID != "" {
		t.Fatalf("userID = %q", platform.userID)
	}
	if platform.data.SessionKey != "dingtalk:g:conv123" {
		t.Fatalf("session key = %q", platform.data.SessionKey)
	}
}

func TestHandleQuestionCardAllowsPlatformDefaultSchemaID(t *testing.T) {
	platform := &stubQuestionCardPlatform{stubPlatformEngine: stubPlatformEngine{n: "dingtalk"}}
	engine := NewEngine("test", &stubAgent{}, []Platform{platform}, "", LangEnglish)
	api := &APIServer{engines: map[string]*Engine{"test": engine}}

	reqBody := QuestionCardRequest{
		Platform: "dingtalk",
		UserID:   "123456",
		CardData: QuestionCardData{
			WorkspaceID: "ws-1",
			QuestionID:  "q-1",
			Question:    "Use which option?",
		},
	}
	body, err := json.Marshal(reqBody)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/cards/question", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	api.handleQuestionCard(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	if platform.schemaID != "" {
		t.Fatalf("schemaID = %q", platform.schemaID)
	}
}

func TestHandleQuestionCardRequiresWorkspaceID(t *testing.T) {
	platform := &stubQuestionCardPlatform{stubPlatformEngine: stubPlatformEngine{n: "dingtalk"}}
	engine := NewEngine("test", &stubAgent{}, []Platform{platform}, "", LangEnglish)
	api := &APIServer{engines: map[string]*Engine{"test": engine}}

	reqBody := QuestionCardRequest{
		Platform: "dingtalk",
		UserID:   "123456",
		SchemaID: "schema-1",
		CardData: QuestionCardData{
			QuestionID: "q-1",
			Question:   "Use which option?",
		},
	}
	body, err := json.Marshal(reqBody)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/cards/question", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	api.handleQuestionCard(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "card_data.workspace_id") {
		t.Fatalf("body = %q", rec.Body.String())
	}
}
