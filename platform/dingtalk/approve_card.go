package dingtalk

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/chenhg5/cc-connect/core"

	"github.com/open-dingtalk/dingtalk-stream-sdk-go/card"
)

// defaultApprovalCardTemplateID is the schema ID for the permission-approval card.
// It can be overridden via the "approval_card_template_id" option.
const defaultApprovalCardTemplateID = "fc2ec48d-3d7b-4f9a-a789-9eaa3d0104df.schema"

// Action values returned by the approval card buttons (matches card schema params.action).
const (
	approvalActionAgree    = "agree"
	approvalActionAllAgree = "all_agree"
	approvalActionReject   = "reject"
)

// approvalParamName is the name of the action parameter on each card button.
const approvalParamName = "action"

// pendingApproval tracks an in-flight permission approval card so that the
// callback handler can route the user's decision back to the correct session.
// A resolved entry is kept (subject to TTL) so duplicate clicks on the same
// card return the original decision idempotently instead of erroring.
type pendingApproval struct {
	sessionKey     string
	replyCtx       replyContext
	createdAt      time.Time
	resolvedAt     time.Time // zero value = not yet resolved
	resolvedAction string    // empty until resolved; one of approvalAction*
}

// permButtonData is the data string the engine emits for each permission button.
// These constants mirror core/engine.go sendPermissionPrompt button values.
const (
	permButtonAllow    = "perm:allow"
	permButtonDeny     = "perm:deny"
	permButtonAllowAll = "perm:allow_all"
)

// containsPermButtons returns true when the button rows look like the
// permission prompt emitted by the engine (allow/deny/allow_all).
func containsPermButtons(buttons [][]core.ButtonOption) bool {
	seen := map[string]bool{}
	for _, row := range buttons {
		for _, b := range row {
			seen[b.Data] = true
		}
	}
	return seen[permButtonAllow] || seen[permButtonDeny] || seen[permButtonAllowAll]
}

// SendWithButtons implements core.InlineButtonSender. When the buttons match
// the permission prompt set (perm:allow / perm:deny / perm:allow_all) AND the
// platform has an approval card template configured, it delivers the request
// as a rich approval card. For any other button set, it returns ErrNotSupported
// so the engine can fall back to the plain-text path.
func (p *Platform) SendWithButtons(ctx context.Context, rctx any, content string, buttons [][]core.ButtonOption) error {
	if !containsPermButtons(buttons) {
		return core.ErrNotSupported
	}
	if p.approvalCardTemplateID == "" {
		return core.ErrNotSupported
	}
	rc, ok := rctx.(replyContext)
	if !ok {
		return fmt.Errorf("dingtalk: SendWithButtons: invalid reply context type %T", rctx)
	}
	if p.isCardDegraded() {
		return fmt.Errorf("dingtalk: approval card API temporarily degraded")
	}

	title, message := splitApprovalPrompt(content)
	createTime := time.Now().Format("2006-01-02 15:04:05")

	if _, err := p.createApprovalCard(ctx, rc, title, message, createTime); err != nil {
		return fmt.Errorf("dingtalk: create approval card: %w", err)
	}
	return nil
}

// splitApprovalPrompt separates the prompt into a short title (the first
// non-empty line) and a markdown body (the rest). DingTalk approval cards
// have a dedicated title slot, so the body should not duplicate it.
func splitApprovalPrompt(content string) (title, message string) {
	content = strings.TrimSpace(content)
	if content == "" {
		return "Permission request", ""
	}
	lines := strings.SplitN(content, "\n", 2)
	title = strings.TrimSpace(stripMarkdownHeading(lines[0]))
	if title == "" {
		title = "Permission request"
	}
	if len(lines) == 2 {
		message = strings.TrimSpace(lines[1])
	}
	return title, message
}

// stripMarkdownHeading removes leading '#' / '>' / spaces from a line so the
// card title doesn't render markdown noise.
func stripMarkdownHeading(line string) string {
	line = strings.TrimSpace(line)
	for strings.HasPrefix(line, "#") {
		line = strings.TrimPrefix(line, "#")
	}
	line = strings.TrimPrefix(line, ">")
	return strings.TrimSpace(line)
}

// createApprovalCard creates and delivers an approval-style interactive card
// using the configured approval template. Returns the outTrackId used to
// identify the card in subsequent callbacks.
func (p *Platform) createApprovalCard(ctx context.Context, rc replyContext, title, message, createTime string) (string, error) {
	token, err := p.getAccessToken()
	if err != nil {
		return "", fmt.Errorf("get access token: %w", err)
	}

	outTrackId := generateOutTrackID()
	isGroup := rc.isGroup
	openSpaceId := openSpaceIDFor(rc)

	cardParamMap := buildApprovalCardParams(title, message, createTime)

	payload := map[string]any{
		"cardTemplateId": p.approvalCardTemplateID,
		"outTrackId":     outTrackId,
		"cardData": map[string]any{
			"cardParamMap": cardParamMap,
		},
		"callbackType":          "STREAM",
		"imGroupOpenSpaceModel": map[string]any{"supportForward": true},
		"imRobotOpenSpaceModel": map[string]any{"supportForward": true},
		"openSpaceId":           openSpaceId,
		"userIdType":            1,
	}

	if isGroup {
		payload["imGroupOpenDeliverModel"] = map[string]any{
			"robotCode": p.robotCode,
		}
	} else {
		payload["imRobotOpenDeliverModel"] = map[string]any{
			"spaceType": "IM_ROBOT",
			"robotCode": p.robotCode,
		}
	}

	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal payload: %w", err)
	}

	reqCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost,
		"https://api.dingtalk.com/v1.0/card/instances/createAndDeliver",
		bytes.NewReader(bodyBytes))
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-acs-dingtalk-access-token", token)

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == 403 || resp.StatusCode == 429 || resp.StatusCode >= 500 {
			p.activateCardDegrade(fmt.Sprintf("approval_card.create:%d", resp.StatusCode))
		}
		return "", fmt.Errorf("create approval card: status=%d, body=%s", resp.StatusCode, string(respBody))
	}

	// Parse response to confirm delivery.
	var deliverCheck struct {
		Result struct {
			DeliverResults []struct {
				Success  bool   `json:"success"`
				ErrorMsg string `json:"errorMsg"`
			} `json:"deliverResults"`
		} `json:"result"`
	}
	if jerr := json.Unmarshal(respBody, &deliverCheck); jerr == nil {
		for _, dr := range deliverCheck.Result.DeliverResults {
			if !dr.Success {
				return "", fmt.Errorf("approval card delivery failed: %s", dr.ErrorMsg)
			}
		}
	}

	// Track the pending approval so the callback can route it back to the session.
	p.trackPendingApproval(outTrackId, pendingApproval{
		sessionKey: rc.sessionKey(p.shareSessionInChannel),
		replyCtx:   rc,
		createdAt:  time.Now(),
	})

	slog.Info("dingtalk: approval card created",
		"outTrackId", outTrackId,
		"isGroup", isGroup,
		"title", title)
	return outTrackId, nil
}

// buildApprovalCardParams converts the typed fields into the DingTalk
// cardParamMap (all values must be strings). Only public schema variables
// are seeded here — errorMessage is private and stays empty until the
// callback handler sets it for a specific user via UserPrivateData.
func buildApprovalCardParams(title, message, createTime string) map[string]string {
	summary := title
	if summary == "" {
		summary = "Permission request"
	}
	return map[string]string{
		"title":      title,
		"message":    message,
		"createTime": createTime,
		"summary":    summary,
		// status is intentionally empty so the original buttons stay visible
		// until the user clicks one (the schema's notEqual conditions match).
		"status": "",
	}
}

// trackPendingApproval registers an in-flight approval card by outTrackId.
func (p *Platform) trackPendingApproval(outTrackId string, ap pendingApproval) {
	p.approvalMu.Lock()
	defer p.approvalMu.Unlock()
	if p.pendingApprovals == nil {
		p.pendingApprovals = make(map[string]pendingApproval)
	}
	p.pruneExpiredApprovalsLocked()
	p.pendingApprovals[outTrackId] = ap
}

// lookupPendingApproval returns the pending approval for an outTrackId.
func (p *Platform) lookupPendingApproval(outTrackId string) (pendingApproval, bool) {
	p.approvalMu.RLock()
	defer p.approvalMu.RUnlock()
	ap, ok := p.pendingApprovals[outTrackId]
	return ap, ok
}

// clearPendingApproval removes a tracked approval entirely. Used by tests and
// teardown paths; the normal callback flow keeps resolved entries around for
// idempotent duplicate-click handling and relies on TTL pruning.
func (p *Platform) clearPendingApproval(outTrackId string) {
	p.approvalMu.Lock()
	defer p.approvalMu.Unlock()
	delete(p.pendingApprovals, outTrackId)
}

// markApprovalResolved records the action that successfully resolved an
// approval card. The entry is kept in the map (subject to approvalRetention)
// so a subsequent click on the same outTrackId can return the same action
// without re-dispatching to the agent.
func (p *Platform) markApprovalResolved(outTrackId, action string) {
	p.approvalMu.Lock()
	defer p.approvalMu.Unlock()
	ap, ok := p.pendingApprovals[outTrackId]
	if !ok {
		return
	}
	ap.resolvedAt = time.Now()
	ap.resolvedAction = action
	p.pendingApprovals[outTrackId] = ap
}

// approvalRetention bounds how long a pending approval is kept around in case
// the user never clicks. After this, the entry is GC'd at the next write.
const approvalRetention = 24 * time.Hour

func (p *Platform) pruneExpiredApprovalsLocked() {
	now := time.Now()
	for id, ap := range p.pendingApprovals {
		if now.Sub(ap.createdAt) > approvalRetention {
			delete(p.pendingApprovals, id)
		}
	}
}

// approvalActionToResponseText maps an approval card action value to the text
// the engine's permission handler expects. Returns ("", false) for unknowns.
func approvalActionToResponseText(action string) (string, bool) {
	switch action {
	case approvalActionAgree:
		return "allow", true
	case approvalActionAllAgree:
		return "allow all", true
	case approvalActionReject:
		return "deny", true
	default:
		return "", false
	}
}

// onCardCallback is invoked by the DingTalk stream SDK when a user clicks a
// button on a card we delivered. It maps the click to a permission decision,
// forwards it to the engine via the normal MessageHandler, and replies with a
// CardResponse that flips the card UI into its disabled (post-decision) state.
//
// Response contract:
//   - Success → CardData.status = <action> (public; flips the schema's
//     visibility condition to show the matching disabled button) and
//     UserPrivateData.errorMessage = "" (private; clears any previous error
//     for this clicker).
//   - Failure → CardData.status = "" (public; keeps the original buttons
//     visible so the user can retry) and UserPrivateData.errorMessage = <err>
//     (private; only the clicker sees it).
//   - Duplicate click on an already-resolved card → returns the success
//     response with the original action, so the operation is idempotent.
func (p *Platform) onCardCallback(ctx context.Context, req *card.CardRequest) (*card.CardResponse, error) {
	if req == nil {
		return approvalFailureResponse("invalid card request"), nil
	}
	action := req.GetActionString(approvalParamName)
	if action == "" {
		slog.Debug("dingtalk: card callback without action", "outTrackId", req.OutTrackId)
		return approvalFailureResponse("missing action parameter"), nil
	}

	responseText, ok := approvalActionToResponseText(action)
	if !ok {
		slog.Warn("dingtalk: unknown approval action", "action", action, "outTrackId", req.OutTrackId)
		return approvalFailureResponse(fmt.Sprintf("unknown action: %s", action)), nil
	}

	pending, found := p.lookupPendingApproval(req.OutTrackId)
	if !found {
		slog.Warn("dingtalk: approval callback for unknown card",
			"outTrackId", req.OutTrackId,
			"action", action)
		return approvalFailureResponse("approval session expired"), nil
	}

	// Idempotency: if cc-connect already processed this request, echo back the
	// original action so the card UI converges to the same final state and
	// the agent isn't re-notified.
	if pending.resolvedAction != "" {
		slog.Debug("dingtalk: approval callback for already-resolved card",
			"outTrackId", req.OutTrackId,
			"originalAction", pending.resolvedAction,
			"newAction", action)
		return approvalSuccessResponse(pending.resolvedAction), nil
	}

	rc := pending.replyCtx
	// The original card may have been delivered without sessionWebhook; mark
	// the synthetic message as proactive so downstream send paths use the
	// correct API.
	rc.proactive = true

	// Resolve a display user name (fall back to the click sender if absent).
	userName := req.UserId
	if pending.replyCtx.senderStaffId != "" {
		userName = pending.replyCtx.senderStaffId
	}

	msg := &core.Message{
		SessionKey: pending.sessionKey,
		Platform:   "dingtalk",
		UserID:     req.UserId,
		UserName:   userName,
		Content:    responseText,
		MessageID:  req.OutTrackId,
		ChannelKey: rc.conversationId,
		ReplyCtx:   rc,
	}

	if p.handler == nil {
		return approvalFailureResponse("platform not started"), nil
	}

	// Forward to the engine in a goroutine so the callback can return quickly.
	// The engine's permission flow is the source of truth — we just need to
	// ensure the synthetic message is delivered before we return ok.
	go p.handler(p, msg)

	// Mark resolved so duplicate clicks on the same card are idempotent.
	// The entry remains in the map until approvalRetention elapses.
	p.markApprovalResolved(req.OutTrackId, action)

	return approvalSuccessResponse(action), nil
}

// approvalSuccessResponse builds a CardResponse for a successfully-handled
// click. CardData.status (public) is set to the action so the schema's
// equality conditions reveal the matching disabled "已允许 / 已允许全部 / 已拒绝"
// button. UserPrivateData.errorMessage (private) is cleared so any stale
// error from a previous failed attempt disappears for this clicker.
func approvalSuccessResponse(action string) *card.CardResponse {
	return &card.CardResponse{
		CardUpdateOptions: &card.CardUpdateOptions{
			UpdateCardDataByKey:    true,
			UpdatePrivateDataByKey: true,
		},
		CardData: &card.CardDataDto{
			CardParamMap: map[string]string{
				"status": action,
			},
		},
		UserPrivateData: &card.CardDataDto{
			CardParamMap: map[string]string{
				"errorMessage": "",
			},
		},
	}
}

// approvalFailureResponse builds a CardResponse for a click that cc-connect
// could not honour (unknown action, expired session, internal error, …).
// CardData.status (public) is set to an empty string so the schema falls
// back to the "original buttons" branch — the user can try again.
// UserPrivateData.errorMessage (private) surfaces the failure reason and is
// only visible to the user who clicked.
func approvalFailureResponse(errMessage string) *card.CardResponse {
	return &card.CardResponse{
		CardUpdateOptions: &card.CardUpdateOptions{
			UpdateCardDataByKey:    true,
			UpdatePrivateDataByKey: true,
		},
		CardData: &card.CardDataDto{
			CardParamMap: map[string]string{
				"status": "",
			},
		},
		UserPrivateData: &card.CardDataDto{
			CardParamMap: map[string]string{
				"errorMessage": errMessage,
			},
		},
	}
}

// sessionKey reconstructs the engine session key used when the card was
// initially delivered. The format matches onMessage so handlePendingPermission
// finds the correct interactive state.
func (rc replyContext) sessionKey(shareInChannel bool) string {
	convType := "d"
	if rc.isGroup {
		convType = "g"
	}
	if shareInChannel {
		return fmt.Sprintf("dingtalk:%s:%s", convType, rc.conversationId)
	}
	return fmt.Sprintf("dingtalk:%s:%s:%s", convType, rc.conversationId, rc.senderStaffId)
}

