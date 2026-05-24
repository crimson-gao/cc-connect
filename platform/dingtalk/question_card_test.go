package dingtalk

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/chenhg5/cc-connect/core"
	"github.com/open-dingtalk/dingtalk-stream-sdk-go/card"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestSendQuestionCard_DeliversToUserOpenSpace(t *testing.T) {
	var gotBody map[string]any
	p := &Platform{
		robotCode:   "dingbot",
		accessToken: "token",
		tokenExpiry: time.Now().Add(time.Hour),
		httpClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.URL.String() != "https://api.dingtalk.com/v1.0/card/instances/createAndDeliver" {
				t.Fatalf("url = %s", req.URL.String())
			}
			if req.Header.Get("x-acs-dingtalk-access-token") != "token" {
				t.Fatalf("token header = %q", req.Header.Get("x-acs-dingtalk-access-token"))
			}
			raw, _ := io.ReadAll(req.Body)
			if err := json.Unmarshal(raw, &gotBody); err != nil {
				t.Fatalf("decode body: %v", err)
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`{"result":{"deliverResults":[{"success":true}]}}`)),
				Header:     make(http.Header),
			}, nil
		})},
	}

	err := p.SendQuestionCard(context.Background(), "123456", "schema-1", core.QuestionCardData{
		WorkspaceID: "ws-1",
		IssueID:     "issue-1",
		IssueTitle:  "Fix login",
		QuestionURL: "https://app.example.com/ws/questions?issueId=issue-1&questionId=q-1",
		QuestionID:  "q-1",
		Question:    "Use which branch?",
		AgentName:   "Agent A",
		Options: []core.QuestionCardOption{
			{Index: 0, Value: "0", Label: "main"},
			{Index: 1, Value: "1", Label: "release"},
		},
	}, map[string]string{"recipient_user_id": "u-1"})
	if err != nil {
		t.Fatalf("SendQuestionCard: %v", err)
	}

	if gotBody["cardTemplateId"] != "schema-1" {
		t.Fatalf("cardTemplateId = %#v", gotBody["cardTemplateId"])
	}
	if gotBody["openSpaceId"] != "dtv1.card//IM_ROBOT.123456" {
		t.Fatalf("openSpaceId = %#v", gotBody["openSpaceId"])
	}
	cardData := gotBody["cardData"].(map[string]any)
	paramMap := cardData["cardParamMap"].(map[string]any)
	if paramMap["issueTitle"] != "Fix login" {
		t.Fatalf("issueTitle = %#v", paramMap["issueTitle"])
	}
	if paramMap["title"] != "Use which branch?" {
		t.Fatalf("title = %#v", paramMap["title"])
	}
	if paramMap["questionUrl"] == "" || paramMap["options"] == "" {
		t.Fatalf("cardParamMap missing questionUrl/options: %#v", paramMap)
	}
	var renderedOptions []map[string]string
	if err := json.Unmarshal([]byte(paramMap["options"].(string)), &renderedOptions); err != nil {
		t.Fatalf("decode rendered options: %v", err)
	}
	if len(renderedOptions) != 3 {
		t.Fatalf("rendered options = %#v", renderedOptions)
	}
	if renderedOptions[0]["value"] != "main" || renderedOptions[0]["text"] != "main" {
		t.Fatalf("first rendered option = %#v", renderedOptions[0])
	}
	if renderedOptions[2]["value"] != "__customAnswer__" || renderedOptions[2]["text"] == "" {
		t.Fatalf("custom rendered option = %#v", renderedOptions[2])
	}
	if paramMap["status"] != "" {
		t.Fatalf("status should be seeded empty, got %#v", paramMap["status"])
	}
}

func TestNewReadsQuestionCardConfig(t *testing.T) {
	platform, err := New(map[string]any{
		"client_id":                 "client-id",
		"client_secret":             "client-secret",
		"robot_code":                "robot-code",
		"question_card_template_id": "schema-from-config",
		"multica_cli":               "/config/multica",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	p := platform.(*Platform)
	if p.questionCardTemplateID != "schema-from-config" {
		t.Fatalf("questionCardTemplateID = %q", p.questionCardTemplateID)
	}
	if p.multicaCLI != "/config/multica" {
		t.Fatalf("multicaCLI = %q", p.multicaCLI)
	}
}

func TestSendQuestionCard_DeliversToGroupSessionOpenSpace(t *testing.T) {
	var gotBody map[string]any
	p := &Platform{
		robotCode:   "dingbot",
		accessToken: "token",
		tokenExpiry: time.Now().Add(time.Hour),
		httpClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			raw, _ := io.ReadAll(req.Body)
			if err := json.Unmarshal(raw, &gotBody); err != nil {
				t.Fatalf("decode body: %v", err)
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`{"result":{"deliverResults":[{"success":true}]}}`)),
				Header:     make(http.Header),
			}, nil
		})},
	}

	err := p.SendQuestionCard(context.Background(), "", "schema-1", core.QuestionCardData{
		SessionKey: "dingtalk:g:conv123",
		QuestionID: "q-1",
		Question:   "Use which branch?",
		Options:    []core.QuestionCardOption{{Index: 0, Value: "0", Label: "main"}},
	}, nil)
	if err != nil {
		t.Fatalf("SendQuestionCard: %v", err)
	}

	if gotBody["openSpaceId"] != "dtv1.card//IM_GROUP.conv123" {
		t.Fatalf("openSpaceId = %#v", gotBody["openSpaceId"])
	}
	if gotBody["imGroupOpenDeliverModel"] == nil {
		t.Fatalf("missing group deliver model: %#v", gotBody)
	}
	if gotBody["imRobotOpenDeliverModel"] != nil {
		t.Fatalf("unexpected robot deliver model: %#v", gotBody["imRobotOpenDeliverModel"])
	}
	cardData := gotBody["cardData"].(map[string]any)
	paramMap := cardData["cardParamMap"].(map[string]any)
	if _, ok := paramMap["session_key"]; ok {
		t.Fatalf("session_key should stay out of template params: %#v", paramMap)
	}
	if paramMap["title"] != "Use which branch?" || paramMap["options"] == "" {
		t.Fatalf("cardParamMap = %#v", paramMap)
	}
}

func TestSendQuestionCard_UsesConfiguredTemplateID(t *testing.T) {
	var gotBody map[string]any
	p := &Platform{
		robotCode:              "dingbot",
		accessToken:            "token",
		tokenExpiry:            time.Now().Add(time.Hour),
		questionCardTemplateID: "schema-from-config",
		httpClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			raw, _ := io.ReadAll(req.Body)
			if err := json.Unmarshal(raw, &gotBody); err != nil {
				t.Fatalf("decode body: %v", err)
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`{"result":{"deliverResults":[{"success":true}]}}`)),
				Header:     make(http.Header),
			}, nil
		})},
	}

	err := p.SendQuestionCard(context.Background(), "123456", "", core.QuestionCardData{
		QuestionID: "q-1",
		Question:   "Use which branch?",
	}, nil)
	if err != nil {
		t.Fatalf("SendQuestionCard: %v", err)
	}

	if gotBody["cardTemplateId"] != "schema-from-config" {
		t.Fatalf("cardTemplateId = %#v", gotBody["cardTemplateId"])
	}
}

func TestResolveQuestionCardTemplateIDFallbacks(t *testing.T) {
	t.Setenv(questionCardTemplateIDEnv, "")
	t.Setenv(questionCardSchemaIDEnv, "")

	p := &Platform{}
	if got := p.resolveQuestionCardTemplateID(""); got != defaultQuestionCardTemplateID {
		t.Fatalf("default template ID = %q, want %q", got, defaultQuestionCardTemplateID)
	}

	t.Setenv(questionCardTemplateIDEnv, "schema-from-env")
	if got := p.resolveQuestionCardTemplateID(""); got != "schema-from-env" {
		t.Fatalf("env template ID = %q", got)
	}
}

func TestQuestionCardCallbackResolvesViaCLI(t *testing.T) {
	p := &Platform{}
	var gotIndices []int
	entry := &questionEntry{
		userID: "staff1",
		cardData: core.QuestionCardData{
			WorkspaceID: "ws-1",
			QuestionID:  "q-1",
			Options: []core.QuestionCardOption{
				{Index: 0, Value: "0", Label: "main"},
				{Index: 1, Value: "release", Label: "release"},
			},
		},
		resolveCLI: func(_ context.Context, _ *questionEntry, indices []int, custom string) error {
			gotIndices = append([]int(nil), indices...)
			if custom != "" {
				t.Fatalf("custom = %q", custom)
			}
			return nil
		},
	}
	p.questionPending.Store("ot1", entry)

	req := questionCardRequest("ot1", "staff1", map[string]any{"action": "release"})
	resp, err := p.onCardCallback(context.Background(), req)
	if err != nil {
		t.Fatalf("onCardCallback: %v", err)
	}
	if resp.CardData == nil || resp.CardData.CardParamMap["status"] != "ok" {
		t.Fatalf("response = %+v", resp)
	}
	if len(gotIndices) != 1 || gotIndices[0] != 1 {
		t.Fatalf("indices = %#v", gotIndices)
	}
	if _, ok := p.questionPending.Load("ot1"); !ok {
		t.Fatal("entry should be retained so duplicate resolved callbacks can show success")
	}
}

func TestQuestionCardCallbackExtractsDingTalkTemplateParams(t *testing.T) {
	p := &Platform{}
	var gotIndices []int
	entry := &questionEntry{
		cardData: core.QuestionCardData{
			WorkspaceID: "ws-1",
			QuestionID:  "q-1",
			Options: []core.QuestionCardOption{
				{Index: 0, Value: "0", Label: "main"},
				{Index: 1, Value: "1", Label: "release"},
			},
		},
		resolveCLI: func(_ context.Context, _ *questionEntry, indices []int, custom string) error {
			gotIndices = append([]int(nil), indices...)
			if custom != "extra context" {
				t.Fatalf("custom = %q", custom)
			}
			return nil
		},
	}
	p.questionPending.Store("ot1", entry)

	resp, err := p.onCardCallback(context.Background(), questionCardRequest("ot1", "staff1", map[string]any{
		"chosenOption": "1",
		"customAnswer": "extra context",
	}))
	if err != nil {
		t.Fatalf("onCardCallback: %v", err)
	}
	if resp.CardData == nil || resp.CardData.CardParamMap["status"] != "ok" {
		t.Fatalf("response = %+v", resp)
	}
	if len(gotIndices) != 1 || gotIndices[0] != 1 {
		t.Fatalf("indices = %#v", gotIndices)
	}
}

func TestMulticaQuestionAnswerArgsUseCardPayloadWorkspace(t *testing.T) {
	entry := &questionEntry{
		cardData: core.QuestionCardData{
			WorkspaceID: "ws-from-card",
			QuestionID:  "q-from-card",
		},
	}

	args, err := multicaQuestionAnswerArgs(entry, []int{0, 2}, "extra")
	if err != nil {
		t.Fatalf("multicaQuestionAnswerArgs: %v", err)
	}

	want := []string{
		"question", "answer", "q-from-card",
		"--workspace-id", "ws-from-card",
		"--option", "0",
		"--option", "2",
		"--custom", "extra",
		"--output", "json",
	}
	if !reflect.DeepEqual(args, want) {
		t.Fatalf("args = %#v, want %#v", args, want)
	}
}

func TestMulticaQuestionAnswerArgsRequiresCardPayloadWorkspace(t *testing.T) {
	entry := &questionEntry{
		cardData: core.QuestionCardData{
			QuestionID: "q-from-card",
		},
	}

	_, err := multicaQuestionAnswerArgs(entry, []int{0}, "")
	if err == nil || !strings.Contains(err.Error(), "card_data.workspace_id") {
		t.Fatalf("error = %v", err)
	}
}

func TestMulticaCLIPathUsesPATHByDefault(t *testing.T) {
	t.Setenv(multicaCLIEnv, "")
	t.Setenv(multicaCLIPathEnv, "")

	if got := multicaCLIPath(nil); got != defaultMulticaCLI {
		t.Fatalf("multicaCLIPath() = %q, want %q", got, defaultMulticaCLI)
	}
}

func TestMulticaCLIPathUsesEnvOverride(t *testing.T) {
	t.Setenv(multicaCLIEnv, "/opt/multica/bin/multica")
	t.Setenv(multicaCLIPathEnv, "/ignored/multica")

	if got := multicaCLIPath(nil); got != "/opt/multica/bin/multica" {
		t.Fatalf("multicaCLIPath() = %q", got)
	}
}

func TestMulticaCLIPathUsesConfigOverride(t *testing.T) {
	t.Setenv(multicaCLIEnv, "/ignored/multica")
	t.Setenv(multicaCLIPathEnv, "/also-ignored/multica")

	entry := &questionEntry{multicaCLI: "/config/multica"}
	if got := multicaCLIPath(entry); got != "/config/multica" {
		t.Fatalf("multicaCLIPath() = %q", got)
	}
}

func TestQuestionCardCallbackAlreadyResolvedIsSuccess(t *testing.T) {
	p := &Platform{}
	entry := &questionEntry{
		userID: "staff1",
		cardData: core.QuestionCardData{
			WorkspaceID: "ws-1",
			QuestionID:  "q-1",
			Options:     []core.QuestionCardOption{{Index: 0, Value: "0", Label: "main"}},
		},
		resolveCLI: func(context.Context, *questionEntry, []int, string) error {
			return fmt.Errorf("answer question: conflict: question is no longer pending: status 409")
		},
	}
	p.questionPending.Store("ot1", entry)

	resp, err := p.onCardCallback(context.Background(), questionCardRequest("ot1", "staff1", map[string]any{"action": "0"}))
	if err != nil {
		t.Fatalf("onCardCallback: %v", err)
	}
	if resp.CardData == nil || resp.CardData.CardParamMap["status"] != "ok" {
		t.Fatalf("response = %+v", resp)
	}
	if _, ok := p.questionPending.Load("ot1"); !ok {
		t.Fatal("entry should be retained when CLI reports already resolved")
	}
}

func TestQuestionCardCallbackResolveFailureDoesNotReturnOKAndCanRetry(t *testing.T) {
	p := &Platform{}
	attempts := 0
	entry := &questionEntry{
		userID: "staff1",
		cardData: core.QuestionCardData{
			WorkspaceID: "ws-1",
			QuestionID:  "q-1",
			Options:     []core.QuestionCardOption{{Index: 0, Value: "0", Label: "main"}},
		},
		resolveCLI: func(context.Context, *questionEntry, []int, string) error {
			attempts++
			if attempts == 1 {
				return fmt.Errorf("temporary multica cli failure")
			}
			return nil
		},
	}
	p.questionPending.Store("ot1", entry)

	resp, err := p.onCardCallback(context.Background(), questionCardRequest("ot1", "staff1", map[string]any{"action": "0"}))
	if err != nil {
		t.Fatalf("first onCardCallback: %v", err)
	}
	if resp.CardData != nil && resp.CardData.CardParamMap["status"] == "ok" {
		t.Fatalf("failed resolve must not return status ok: %+v", resp)
	}

	resp, err = p.onCardCallback(context.Background(), questionCardRequest("ot1", "staff1", map[string]any{"action": "0"}))
	if err != nil {
		t.Fatalf("retry onCardCallback: %v", err)
	}
	if resp.CardData == nil || resp.CardData.CardParamMap["status"] != "ok" {
		t.Fatalf("retry response = %+v", resp)
	}
	if attempts != 2 {
		t.Fatalf("attempts=%d, want 2", attempts)
	}
}

func TestQuestionCardCallbackInFlightDoesNotReturnOK(t *testing.T) {
	p := &Platform{}
	started := make(chan struct{})
	release := make(chan struct{})
	entry := &questionEntry{
		userID: "staff1",
		cardData: core.QuestionCardData{
			WorkspaceID: "ws-1",
			QuestionID:  "q-1",
			Options:     []core.QuestionCardOption{{Index: 0, Value: "0", Label: "main"}},
		},
		resolveCLI: func(context.Context, *questionEntry, []int, string) error {
			close(started)
			<-release
			return nil
		},
	}
	p.questionPending.Store("ot1", entry)

	done := make(chan *card.CardResponse, 1)
	go func() {
		resp, _ := p.onCardCallback(context.Background(), questionCardRequest("ot1", "staff1", map[string]any{"action": "0"}))
		done <- resp
	}()
	<-started

	resp, err := p.onCardCallback(context.Background(), questionCardRequest("ot1", "staff1", map[string]any{"action": "0"}))
	if err != nil {
		t.Fatalf("second onCardCallback: %v", err)
	}
	if resp.CardData != nil && resp.CardData.CardParamMap["status"] == "ok" {
		t.Fatalf("in-flight resolve must not return status ok: %+v", resp)
	}

	close(release)
	final := <-done
	if final.CardData == nil || final.CardData.CardParamMap["status"] != "ok" {
		t.Fatalf("final response = %+v", final)
	}
}

func questionCardRequest(outTrackID, userID string, params map[string]any) *card.CardRequest {
	return &card.CardRequest{
		OutTrackId: outTrackID,
		UserId:     userID,
		CardActionData: card.PrivateCardActionData{
			CardPrivateData: card.CardPrivateData{Params: params},
		},
	}
}
