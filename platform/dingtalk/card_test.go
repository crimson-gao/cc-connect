package dingtalk

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

type cardHTTPCall struct {
	method string
	path   string
	body   map[string]any
}

type cardRoundTripper struct {
	t             *testing.T
	calls         []cardHTTPCall
	failStreaming bool
}

func (rt *cardRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	bodyBytes, _ := io.ReadAll(req.Body)
	body := map[string]any{}
	if len(bodyBytes) > 0 {
		if err := json.Unmarshal(bodyBytes, &body); err != nil {
			rt.t.Fatalf("unmarshal request body: %v\n%s", err, string(bodyBytes))
		}
	}
	rt.calls = append(rt.calls, cardHTTPCall{
		method: req.Method,
		path:   req.URL.Path,
		body:   body,
	})

	status := http.StatusOK
	respBody := `{"result":{"cardInstanceId":"card-1","outTrackId":"out-1","deliverResults":[{"success":true}]}}`
	if rt.failStreaming && req.URL.Path == "/v1.0/card/streaming" {
		status = http.StatusInternalServerError
		respBody = `{"code":"InternalError","message":"stream failed"}`
	}

	return &http.Response{
		StatusCode: status,
		Body:       io.NopCloser(strings.NewReader(respBody)),
		Header:     make(http.Header),
	}, nil
}

func withFastCardLimiter(t *testing.T) {
	t.Helper()
	orig := cardLimiter
	cardLimiter = newTokenBucket(1000, 1000, 0)
	t.Cleanup(func() {
		cardLimiter = orig
	})
}

func cardParams(t *testing.T, body map[string]any) map[string]any {
	t.Helper()
	cardData, ok := body["cardData"].(map[string]any)
	if !ok {
		t.Fatalf("cardData missing or wrong type: %#v", body["cardData"])
	}
	params, ok := cardData["cardParamMap"].(map[string]any)
	if !ok {
		t.Fatalf("cardParamMap missing or wrong type: %#v", cardData["cardParamMap"])
	}
	return params
}

func TestNewDefaultsToDynamicPublicAICard(t *testing.T) {
	platform, err := New(map[string]any{
		"client_id":     "client",
		"client_secret": "secret",
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	p := platform.(*Platform)
	if !p.dynamicCard {
		t.Fatal("dynamicCard = false, want true by default")
	}
	if !p.useDefaultTemplate {
		t.Fatal("useDefaultTemplate = false, want true when card_template_id is empty")
	}
	if p.cardTemplateID != DefaultCardTemplateID {
		t.Fatalf("cardTemplateID = %q, want %q", p.cardTemplateID, DefaultCardTemplateID)
	}
	if p.cardTemplateKey != "msgContent" {
		t.Fatalf("cardTemplateKey = %q, want msgContent", p.cardTemplateKey)
	}
}

func TestNewCustomAICardTemplateDefaultsToContentKey(t *testing.T) {
	platform, err := New(map[string]any{
		"client_id":        "client",
		"client_secret":    "secret",
		"card_template_id": "custom.schema",
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	p := platform.(*Platform)
	if p.useDefaultTemplate {
		t.Fatal("useDefaultTemplate = true, want false for custom template")
	}
	if p.cardTemplateID != "custom.schema" {
		t.Fatalf("cardTemplateID = %q, want custom.schema", p.cardTemplateID)
	}
	if p.cardTemplateKey != "content" {
		t.Fatalf("cardTemplateKey = %q, want content", p.cardTemplateKey)
	}
}

func TestCreateStreamingCardDisabledByConfig(t *testing.T) {
	platform, err := New(map[string]any{
		"client_id":     "client",
		"client_secret": "secret",
		"dynamic_card":  false,
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	_, err = platform.(*Platform).CreateStreamingCard(context.Background(), replyContext{})
	if err == nil || !strings.Contains(err.Error(), "dynamic_card disabled") {
		t.Fatalf("CreateStreamingCard error = %v, want dynamic_card disabled", err)
	}
}

func TestCreateAICardUsesPublicTemplateParamsAndDirectSpaceID(t *testing.T) {
	withFastCardLimiter(t)
	rt := &cardRoundTripper{t: t}
	p := &Platform{
		robotCode:          "robot",
		httpClient:         &http.Client{Transport: rt},
		accessToken:        "token",
		tokenExpiry:        time.Now().Add(time.Hour),
		cardTemplateID:     DefaultCardTemplateID,
		cardTemplateKey:    "msgContent",
		useDefaultTemplate: true,
		cardThrottleMs:     300,
	}

	card, err := p.createAICard(context.Background(), replyContext{
		conversationId: "cid-direct",
		senderStaffId:  "staff-42",
		isGroup:        false,
	})
	if err != nil {
		t.Fatalf("createAICard() error: %v", err)
	}
	if card.outTrackId != "out-1" {
		t.Fatalf("outTrackId = %q, want out-1", card.outTrackId)
	}
	if len(rt.calls) != 1 {
		t.Fatalf("calls = %d, want 1", len(rt.calls))
	}
	call := rt.calls[0]
	if call.method != http.MethodPost || call.path != "/v1.0/card/instances/createAndDeliver" {
		t.Fatalf("call = %s %s, want POST createAndDeliver", call.method, call.path)
	}
	if call.body["cardTemplateId"] != DefaultCardTemplateID {
		t.Fatalf("cardTemplateId = %v, want public template", call.body["cardTemplateId"])
	}
	if call.body["openSpaceId"] != "dtv1.card//IM_ROBOT.staff-42" {
		t.Fatalf("openSpaceId = %v, want direct staff space", call.body["openSpaceId"])
	}
	params := cardParams(t, call.body)
	for _, key := range []string{"flowStatus", "msgContent", "staticMsgContent", "sys_full_json_obj", "config"} {
		if _, ok := params[key]; !ok {
			t.Fatalf("public template params missing %q: %#v", key, params)
		}
	}
	if params["flowStatus"] != cardFlowProcessing {
		t.Fatalf("flowStatus = %v, want processing", params["flowStatus"])
	}
}

func TestAICardFinalizeTransitionsPublicTemplateToFinished(t *testing.T) {
	withFastCardLimiter(t)
	rt := &cardRoundTripper{t: t}
	card := testAICard(rt)

	if err := card.Finalize(context.Background(), "line one\nline two"); err != nil {
		t.Fatalf("Finalize() error: %v", err)
	}
	if card.Failed() {
		t.Fatal("card Failed() = true, want false")
	}
	if len(rt.calls) != 3 {
		t.Fatalf("calls = %d, want INPUTING, streaming, FINISHED", len(rt.calls))
	}

	first := cardParams(t, rt.calls[0].body)
	if rt.calls[0].path != "/v1.0/card/instances" || first["flowStatus"] != cardFlowInputing {
		t.Fatalf("first call = %s params=%#v, want INPUTING instance update", rt.calls[0].path, first)
	}
	stream := rt.calls[1].body
	if rt.calls[1].path != "/v1.0/card/streaming" {
		t.Fatalf("second path = %s, want streaming", rt.calls[1].path)
	}
	if stream["key"] != "msgContent" || stream["isFinalize"] != true || stream["isError"] != false {
		t.Fatalf("stream body = %#v, want final non-error msgContent update", stream)
	}
	if !strings.Contains(stream["content"].(string), "line one<br>line two") {
		t.Fatalf("stream content = %q, want normalized line break", stream["content"])
	}
	last := cardParams(t, rt.calls[2].body)
	if rt.calls[2].path != "/v1.0/card/instances" || last["flowStatus"] != cardFlowFinished {
		t.Fatalf("last call = %s params=%#v, want FINISHED instance update", rt.calls[2].path, last)
	}
}

func TestAICardFailSendsErrorAndStopsLoading(t *testing.T) {
	withFastCardLimiter(t)
	rt := &cardRoundTripper{t: t}
	card := testAICard(rt)

	if err := card.Fail(context.Background(), "boom"); err != nil {
		t.Fatalf("Fail() error: %v", err)
	}
	if !card.Failed() {
		t.Fatal("card Failed() = false, want true")
	}
	if len(rt.calls) != 3 {
		t.Fatalf("calls = %d, want INPUTING, error streaming, FINISHED", len(rt.calls))
	}
	stream := rt.calls[1].body
	if stream["isFinalize"] != true || stream["isError"] != true {
		t.Fatalf("stream body = %#v, want finalized error update", stream)
	}
	last := cardParams(t, rt.calls[2].body)
	if last["flowStatus"] != cardFlowFinished {
		t.Fatalf("final flowStatus = %v, want FINISHED", last["flowStatus"])
	}
}

func TestAICardFinalizeMarksFailedOnStreamError(t *testing.T) {
	withFastCardLimiter(t)
	rt := &cardRoundTripper{t: t, failStreaming: true}
	card := testAICard(rt)

	err := card.Finalize(context.Background(), "answer")
	if err == nil {
		t.Fatal("Finalize() error = nil, want stream failure")
	}
	if !card.Failed() {
		t.Fatal("card Failed() = false, want true after stream failure")
	}
	if !card.platform.isCardDegraded() {
		t.Fatal("platform card degrade = false, want true after 500")
	}
}

func testAICard(rt *cardRoundTripper) *aiCard {
	p := &Platform{
		httpClient:     &http.Client{Transport: rt},
		accessToken:    "token",
		tokenExpiry:    time.Now().Add(time.Hour),
		cardThrottleMs: 0,
	}
	return &aiCard{
		cardInstanceId: "card-1",
		outTrackId:     "out-1",
		templateKey:    "msgContent",
		defaultTpl:     true,
		platform:       p,
		state:          "processing",
		done:           make(chan struct{}),
	}
}
