package dingtalk

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/chenhg5/cc-connect/core"

	"github.com/open-dingtalk/dingtalk-stream-sdk-go/card"
)

const (
	defaultQuestionCardTemplateID = "566bd03d-f291-4dd2-b7ad-ab690b7414c4.schema"
	questionCardTemplateIDEnv     = "CC_CONNECT_QUESTION_CARD_TEMPLATE_ID"
	questionCardSchemaIDEnv       = "CC_CONNECT_QUESTION_CARD_SCHEMA_ID"
	multicaCLIEnv                 = "CC_CONNECT_MULTICA_CLI"
	multicaCLIPathEnv             = "MULTICA_CLI_PATH"
	defaultMulticaCLI             = "multica"
	questionCardEntryTTL          = 24 * time.Hour
)

type questionEntry struct {
	userID     string
	schemaID   string
	cardData   core.QuestionCardData
	metadata   map[string]string
	createdAt  time.Time
	multicaCLI string
	stateMu    sync.Mutex
	state      questionResolveState
	resolveCLI func(context.Context, *questionEntry, []int, string) error
}

type questionResolveState int

const (
	questionResolvePending questionResolveState = iota
	questionResolveInFlight
	questionResolveResolved
)

type questionCardTarget struct {
	openSpaceID string
	userID      string
	isGroup     bool
}

// SendQuestionCard implements core.QuestionCardSender. It delivers a DingTalk
// schema card and registers callback context for resolving the Multica question
// through the local multica CLI.
func (p *Platform) SendQuestionCard(ctx context.Context, userID, schemaID string, data core.QuestionCardData, metadata map[string]string) error {
	schemaID = p.resolveQuestionCardTemplateID(schemaID)
	if schemaID == "" {
		return fmt.Errorf("dingtalk: question card schema_id is required")
	}
	if strings.TrimSpace(userID) == "" && strings.TrimSpace(data.SessionKey) == "" {
		return fmt.Errorf("dingtalk: question card user_id or session_key is required")
	}
	if data.QuestionID == "" {
		return fmt.Errorf("dingtalk: question card question_id is required")
	}
	if p.isCardDegraded() {
		return fmt.Errorf("dingtalk: card API temporarily degraded")
	}

	target, err := p.resolveQuestionCardTarget(userID, data)
	if err != nil {
		return err
	}

	outTrackId := generateQuestionOutTrackID()
	entry := &questionEntry{
		userID:     target.userID,
		schemaID:   schemaID,
		cardData:   data,
		metadata:   cloneStringMap(metadata),
		createdAt:  time.Now(),
		multicaCLI: p.resolveMulticaCLI(),
		resolveCLI: resolveMulticaQuestion,
	}
	p.questionPending.Store(outTrackId, entry)
	if err := p.deliverQuestionCard(ctx, target, schemaID, outTrackId, data, metadata); err != nil {
		p.questionPending.Delete(outTrackId)
		return err
	}
	p.sweepStaleQuestionEntries()
	return nil
}

func (p *Platform) resolveQuestionCardTemplateID(schemaID string) string {
	if schemaID := strings.TrimSpace(schemaID); schemaID != "" {
		return schemaID
	}
	if p != nil {
		if schemaID := strings.TrimSpace(p.questionCardTemplateID); schemaID != "" {
			return schemaID
		}
	}
	if schemaID := strings.TrimSpace(os.Getenv(questionCardTemplateIDEnv)); schemaID != "" {
		return schemaID
	}
	if schemaID := strings.TrimSpace(os.Getenv(questionCardSchemaIDEnv)); schemaID != "" {
		return schemaID
	}
	return defaultQuestionCardTemplateID
}

func (p *Platform) resolveQuestionCardTarget(userID string, data core.QuestionCardData) (questionCardTarget, error) {
	if sessionKey := strings.TrimSpace(data.SessionKey); sessionKey != "" {
		rawCtx, err := p.ReconstructReplyCtx(sessionKey)
		if err != nil {
			return questionCardTarget{}, err
		}
		rc, ok := rawCtx.(replyContext)
		if !ok {
			return questionCardTarget{}, fmt.Errorf("dingtalk: reconstructed question card context has unexpected type %T", rawCtx)
		}
		if rc.isGroup {
			return questionCardTarget{
				openSpaceID: fmt.Sprintf("dtv1.card//IM_GROUP.%s", rc.conversationId),
				isGroup:     true,
			}, nil
		}
		targetUserID := strings.TrimSpace(rc.senderStaffId)
		if targetUserID == "" {
			targetUserID = strings.TrimSpace(userID)
		}
		if targetUserID == "" {
			return questionCardTarget{}, fmt.Errorf("dingtalk: direct question card session key requires sender staff id or user_id")
		}
		return questionCardTarget{
			openSpaceID: fmt.Sprintf("dtv1.card//IM_ROBOT.%s", targetUserID),
			userID:      targetUserID,
		}, nil
	}

	userID = strings.TrimSpace(userID)
	if userID == "" {
		return questionCardTarget{}, fmt.Errorf("dingtalk: question card user_id is required")
	}
	return questionCardTarget{
		openSpaceID: fmt.Sprintf("dtv1.card//IM_ROBOT.%s", userID),
		userID:      userID,
	}, nil
}

func (p *Platform) deliverQuestionCard(ctx context.Context, target questionCardTarget, schemaID, outTrackId string, data core.QuestionCardData, metadata map[string]string) error {
	token, err := p.getAccessToken()
	if err != nil {
		return fmt.Errorf("get access token: %w", err)
	}

	payload := map[string]any{
		"cardTemplateId": schemaID,
		"outTrackId":     outTrackId,
		"cardData": map[string]any{
			"cardParamMap": buildQuestionCardParamMap(data, metadata),
		},
		"callbackType":          "STREAM",
		"imGroupOpenSpaceModel": map[string]any{"supportForward": false},
		"imRobotOpenSpaceModel": map[string]any{"supportForward": false},
		"openSpaceId":           target.openSpaceID,
		"userIdType":            1,
	}
	if target.isGroup {
		payload["imGroupOpenDeliverModel"] = map[string]any{"robotCode": p.robotCode}
	} else {
		payload["imRobotOpenDeliverModel"] = map[string]any{
			"spaceType": "IM_ROBOT",
			"robotCode": p.robotCode,
		}
	}

	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	reqCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost,
		"https://api.dingtalk.com/v1.0/card/instances/createAndDeliver",
		bytes.NewReader(bodyBytes))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-acs-dingtalk-access-token", token)

	slog.Debug("dingtalk: creating question card", "outTrackId", outTrackId, "user_id", target.userID, "is_group", target.isGroup, "question_id", data.QuestionID)
	resp, err := p.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	slog.Debug("dingtalk: question card createAndDeliver response", "status", resp.StatusCode, "body", string(respBody))
	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == 403 || resp.StatusCode == 429 || resp.StatusCode >= 500 {
			p.activateCardDegrade(fmt.Sprintf("questioncard.create:%d", resp.StatusCode))
		}
		return fmt.Errorf("create question card: status=%d, body=%s", resp.StatusCode, string(respBody))
	}

	var deliverCheck struct {
		Result struct {
			DeliverResults []struct {
				Success  bool   `json:"success"`
				ErrorMsg string `json:"errorMsg"`
			} `json:"deliverResults"`
		} `json:"result"`
	}
	if err := json.Unmarshal(respBody, &deliverCheck); err == nil {
		for _, dr := range deliverCheck.Result.DeliverResults {
			if !dr.Success {
				return fmt.Errorf("question card delivery failed: %s", dr.ErrorMsg)
			}
		}
	}
	return nil
}

func buildQuestionCardParamMap(data core.QuestionCardData, metadata map[string]string) map[string]string {
	_ = metadata
	options := renderQuestionCardOptions(data.Options)
	optionsJSON := "[]"
	if raw, err := json.Marshal(options); err == nil {
		optionsJSON = string(raw)
	}
	return map[string]string{
		"title":       questionCardTitle(data),
		"options":     optionsJSON,
		"status":      "",
		"agentName":   firstNonEmpty(data.AgentName, data.AgentNameText),
		"issueTitle":  data.IssueTitle,
		"questionUrl": data.QuestionURL,
	}
}

func questionCardTitle(data core.QuestionCardData) string {
	if strings.TrimSpace(data.Question) != "" {
		return data.Question
	}
	if strings.TrimSpace(data.Header) != "" {
		return data.Header
	}
	return "请选择"
}

type renderedQuestionCardOption struct {
	Value       string `json:"value"`
	Text        string `json:"text"`
	Description string `json:"description"`
}

func renderQuestionCardOptions(options []core.QuestionCardOption) []renderedQuestionCardOption {
	rendered := make([]renderedQuestionCardOption, 0, len(options)+1)
	for idx, opt := range options {
		text := strings.TrimSpace(opt.Label)
		if text == "" {
			text = strings.TrimSpace(opt.Description)
		}
		if text == "" {
			text = strings.TrimSpace(opt.Value)
		}
		if text == "" {
			text = fmt.Sprintf("选项 %d", idx+1)
		}
		rendered = append(rendered, renderedQuestionCardOption{
			Value:       text,
			Text:        text,
			Description: opt.Description,
		})
	}
	rendered = append(rendered, renderedQuestionCardOption{
		Value:       "__customAnswer__",
		Text:        "自定义回答",
		Description: "",
	})
	return rendered
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func (p *Platform) onCardCallback(ctx context.Context, req *card.CardRequest) (*card.CardResponse, error) {
	if req == nil || req.OutTrackId == "" {
		return questionCardErrorResponse("invalid card callback"), nil
	}
	entryAny, ok := p.questionPending.Load(req.OutTrackId)
	if !ok {
		slog.Debug("dingtalk: question card callback for unknown outTrackId", "outTrackId", req.OutTrackId)
		return questionCardErrorResponse("question request expired or already resolved"), nil
	}
	entry, ok := entryAny.(*questionEntry)
	if !ok {
		return questionCardErrorResponse("invalid question request state"), nil
	}
	return p.onQuestionCardCallback(ctx, req, entry)
}

func (p *Platform) onQuestionCardCallback(ctx context.Context, req *card.CardRequest, entry *questionEntry) (*card.CardResponse, error) {
	if entry == nil {
		return questionCardErrorResponse("question request expired"), nil
	}
	if entry.userID != "" && req.UserId != "" && req.UserId != entry.userID {
		slog.Info("dingtalk: question card click from non-recipient user",
			"clicker", req.UserId,
			"recipient", entry.userID,
			"outTrackId", req.OutTrackId)
		return &card.CardResponse{
			UserPrivateData:   &card.CardDataDto{CardParamMap: map[string]string{"hasPermission": "false"}},
			CardUpdateOptions: &card.CardUpdateOptions{UpdatePrivateDataByKey: true},
		}, nil
	}

	optionIndices, customText := extractQuestionAnswer(req, entry.cardData.Options)
	if len(optionIndices) == 0 && customText == "" {
		return questionCardErrorResponse("missing answer"), nil
	}

	switch entry.beginResolve() {
	case questionResolveResolved:
		return questionCardSuccessResponse(optionIndices, customText), nil
	case questionResolveInFlight:
		return questionCardErrorResponse("resolve in progress"), nil
	}

	resolveFn := entry.resolveCLI
	if resolveFn == nil {
		resolveFn = resolveMulticaQuestion
	}
	callCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := resolveFn(callCtx, entry, optionIndices, customText); err != nil {
		if isMulticaAlreadyResolved(err) {
			entry.markResolveResolved()
			return questionCardSuccessResponse(optionIndices, customText), nil
		}
		entry.markResolvePending()
		slog.Warn("dingtalk: resolve question via multica CLI failed",
			"question_id", entry.cardData.QuestionID,
			"outTrackId", req.OutTrackId,
			"error", err)
		return questionCardErrorResponse("resolve failed"), nil
	}

	entry.markResolveResolved()
	return questionCardSuccessResponse(optionIndices, customText), nil
}

func (e *questionEntry) beginResolve() questionResolveState {
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	switch e.state {
	case questionResolveResolved:
		return questionResolveResolved
	case questionResolveInFlight:
		return questionResolveInFlight
	default:
		e.state = questionResolveInFlight
		return questionResolvePending
	}
}

func (e *questionEntry) markResolvePending() {
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	e.state = questionResolvePending
}

func (e *questionEntry) markResolveResolved() {
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	e.state = questionResolveResolved
}

var resolveMulticaQuestion = runMulticaQuestionAnswer

func runMulticaQuestionAnswer(ctx context.Context, entry *questionEntry, optionIndices []int, customText string) error {
	args, err := multicaQuestionAnswerArgs(entry, optionIndices, customText)
	if err != nil {
		return err
	}

	cmd := exec.CommandContext(ctx, multicaCLIPath(entry), args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("multica question answer: %w: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

func (p *Platform) resolveMulticaCLI() string {
	if p == nil {
		return multicaCLIPath(nil)
	}
	return resolveMulticaCLIPath(p.multicaCLI)
}

func multicaCLIPath(entry *questionEntry) string {
	if entry == nil {
		return resolveMulticaCLIPath("")
	}
	return resolveMulticaCLIPath(entry.multicaCLI)
}

func resolveMulticaCLIPath(configured string) string {
	if cliPath := strings.TrimSpace(configured); cliPath != "" {
		return cliPath
	}
	if cliPath := strings.TrimSpace(os.Getenv(multicaCLIEnv)); cliPath != "" {
		return cliPath
	}
	if cliPath := strings.TrimSpace(os.Getenv(multicaCLIPathEnv)); cliPath != "" {
		return cliPath
	}
	return defaultMulticaCLI
}

func multicaQuestionAnswerArgs(entry *questionEntry, optionIndices []int, customText string) ([]string, error) {
	if entry == nil {
		return nil, fmt.Errorf("multica question answer: missing question context")
	}
	questionID := strings.TrimSpace(entry.cardData.QuestionID)
	workspaceID := strings.TrimSpace(entry.cardData.WorkspaceID)
	if questionID == "" {
		return nil, fmt.Errorf("multica question answer: card_data.question_id is required")
	}
	if workspaceID == "" {
		return nil, fmt.Errorf("multica question answer: card_data.workspace_id is required")
	}

	args := []string{
		"question",
		"answer",
		questionID,
		"--workspace-id",
		workspaceID,
	}
	for _, idx := range optionIndices {
		args = append(args, "--option", strconv.Itoa(idx))
	}
	if customText != "" {
		args = append(args, "--custom", customText)
	}
	args = append(args, "--output", "json")
	return args, nil
}

func isMulticaAlreadyResolved(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "no longer pending") ||
		strings.Contains(msg, "already resolved") ||
		strings.Contains(msg, "already answered") ||
		strings.Contains(msg, "status 409") ||
		strings.Contains(msg, "409") ||
		strings.Contains(msg, "conflict")
}

func extractQuestionAnswer(req *card.CardRequest, options []core.QuestionCardOption) ([]int, string) {
	params := map[string]any{}
	if req != nil {
		params = req.CardActionData.CardPrivateData.Params
	}

	var indices []int
	addValue := func(v any) {
		indices = append(indices, parseOptionIndices(v, options)...)
	}
	for _, key := range []string{
		"option", "optionIndex", "option_index", "value", "answer",
		"chosenOption", "chosen_option",
		"selected", "selectedOption", "selected_option",
		"options", "optionIndices", "option_indices", "selectedOptions",
		"selected_options", "values",
	} {
		if v, ok := params[key]; ok {
			addValue(v)
		}
	}
	if req != nil {
		if action := req.GetActionString("action"); action != "" {
			addValue(action)
		}
	}

	customText := firstParamString(params, "custom", "customAnswer", "custom_answer", "customText", "custom_text", "text", "answerText", "answer_text")
	return dedupeInts(indices), customText
}

func parseOptionIndices(v any, options []core.QuestionCardOption) []int {
	switch x := v.(type) {
	case nil:
		return nil
	case int:
		return []int{x}
	case int64:
		return []int{int(x)}
	case float64:
		return []int{int(x)}
	case string:
		return parseOptionString(x, options)
	case []any:
		var out []int
		for _, item := range x {
			out = append(out, parseOptionIndices(item, options)...)
		}
		return out
	case []string:
		var out []int
		for _, item := range x {
			out = append(out, parseOptionString(item, options)...)
		}
		return out
	default:
		raw, _ := json.Marshal(x)
		return parseOptionString(string(raw), options)
	}
}

func parseOptionString(s string, options []core.QuestionCardOption) []int {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	if strings.HasPrefix(s, "[") {
		var arr []any
		if err := json.Unmarshal([]byte(s), &arr); err == nil {
			return parseOptionIndices(arr, options)
		}
	}
	if strings.Contains(s, ",") {
		var out []int
		for _, part := range strings.Split(s, ",") {
			out = append(out, parseOptionString(part, options)...)
		}
		return out
	}
	if idx, err := strconv.Atoi(s); err == nil {
		return []int{idx}
	}
	for _, opt := range options {
		if s == opt.Value || s == opt.Label {
			return []int{opt.Index}
		}
	}
	if matches := regexp.MustCompile(`-?\d+`).FindAllString(s, -1); len(matches) > 0 {
		out := make([]int, 0, len(matches))
		for _, m := range matches {
			if idx, err := strconv.Atoi(m); err == nil {
				out = append(out, idx)
			}
		}
		return out
	}
	return nil
}

func firstParamString(params map[string]any, keys ...string) string {
	for _, key := range keys {
		v, ok := params[key]
		if !ok {
			continue
		}
		switch x := v.(type) {
		case string:
			if strings.TrimSpace(x) != "" {
				return x
			}
		case fmt.Stringer:
			if strings.TrimSpace(x.String()) != "" {
				return x.String()
			}
		}
	}
	return ""
}

func dedupeInts(in []int) []int {
	if len(in) == 0 {
		return nil
	}
	seen := map[int]bool{}
	out := make([]int, 0, len(in))
	for _, v := range in {
		if seen[v] {
			continue
		}
		seen[v] = true
		out = append(out, v)
	}
	return out
}

func questionCardSuccessResponse(optionIndices []int, customText string) *card.CardResponse {
	params := map[string]string{
		"status":                "ok",
		"selectedOptionIndices": joinInts(optionIndices),
	}
	if customText != "" {
		params["customText"] = customText
	}
	return &card.CardResponse{
		CardData:          &card.CardDataDto{CardParamMap: params},
		CardUpdateOptions: &card.CardUpdateOptions{UpdateCardDataByKey: true},
	}
}

func questionCardErrorResponse(msg string) *card.CardResponse {
	return &card.CardResponse{
		CardData:          &card.CardDataDto{CardParamMap: map[string]string{"errorMessage": msg}},
		CardUpdateOptions: &card.CardUpdateOptions{UpdateCardDataByKey: true},
	}
}

func joinInts(values []int) string {
	parts := make([]string, 0, len(values))
	for _, v := range values {
		parts = append(parts, strconv.Itoa(v))
	}
	return strings.Join(parts, ",")
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return map[string]string{}
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func (p *Platform) sweepStaleQuestionEntries() {
	cutoff := time.Now().Add(-questionCardEntryTTL)
	p.questionPending.Range(func(k, v any) bool {
		entry, ok := v.(*questionEntry)
		if ok && entry.createdAt.Before(cutoff) {
			p.questionPending.Delete(k)
		}
		return true
	})
}

func generateQuestionOutTrackID() string {
	return "question_" + generateOutTrackID()[len("card_"):]
}
